#!/usr/bin/env python3
"""
lora_kiss_bridge.py — LoRa APRS → KISS TNC bridge for Direwolf
================================================================
Reads LoRa packets from a supported SX1262 / SX1276 module and exposes
them as a KISS TNC (PTY or TCP socket) for Direwolf to consume as an
iGate or digipeater.

Hardware drivers are provided by the LoRaRF library which has been
directly tested against the E22-900M30S (MeshAdv-Pi Hat) and other
Ebyte E22 modules:
  pip3 install LoRaRF

Supported hardware profiles (defined in hardware_profiles.yaml):
  meshadv        MeshAdv-Pi Hat — Ebyte E22-900M30S (SX1262)
  ebyte_e22      Ebyte E22-xxMxxS generic breakout (SX1262)
  ttgo_uart      TTGO / Heltec over USB-UART KISS passthrough
  generic_sx1276 Ra-02, RFM95W, generic SX1276/SX1278

Usage:
  python3 lora_kiss_bridge.py --profile meshadv

Dependencies:
  pip3 install LoRaRF pyyaml pyserial
"""

import argparse
import logging
import os
import pty
import select
import signal
import socket
import sys
import threading
import time

import yaml

log = logging.getLogger("lora_kiss_bridge")

# ---------------------------------------------------------------------------
# Optional imports — LoRaRF and serial are only needed on real hardware
# ---------------------------------------------------------------------------
try:
    from LoRaRF import SX126x, SX127x  # type: ignore[import]
    LORALIB_AVAILABLE = True
except ImportError:
    LORALIB_AVAILABLE = False

try:
    import serial as pyserial
    SERIAL_AVAILABLE = True
except ImportError:
    SERIAL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Patch rpi-lgpio 0.6 bug: setup(pin, OUT) calls gpio_read() on an unclaimed
# pin, raising 'GPIO not allocated'.  Providing initial=0 skips that read.
# ---------------------------------------------------------------------------
try:
    import RPi.GPIO as _GPIO  # type: ignore[import]
    # Release any stale GPIO claims from a previous crashed run,
    # then restore BCM mode which cleanup() resets.
    _GPIO.cleanup()
    _GPIO.setmode(_GPIO.BCM)
    _orig_gpio_setup = _GPIO.setup
    def _gpio_setup_fixed(channel, direction, **kwargs):
        if direction == _GPIO.OUT and 'initial' not in kwargs:
            kwargs['initial'] = 0
        return _orig_gpio_setup(channel, direction, **kwargs)
    _GPIO.setup = _gpio_setup_fixed
except Exception:
    pass


# ===========================================================================
# KISS framing
# ===========================================================================

FEND  = 0xC0
FESC  = 0xDB
TFEND = 0xDC
TFESC = 0xDD
KISS_DATA_FRAME = 0x00


def kiss_encode(data: bytes) -> bytes:
    """Wrap raw AX.25 bytes in a KISS data frame."""
    escaped = bytearray()
    for byte in data:
        if byte == FEND:
            escaped += bytes([FESC, TFEND])
        elif byte == FESC:
            escaped += bytes([FESC, TFESC])
        else:
            escaped.append(byte)
    return bytes([FEND, KISS_DATA_FRAME]) + bytes(escaped) + bytes([FEND])


def kiss_decode(raw: bytes) -> list:
    """Parse one or more KISS frames from a raw byte buffer.
    Returns a list of decoded AX.25 payload byte strings."""
    frames = []
    i = 0
    while i < len(raw):
        if raw[i] != FEND:
            i += 1
            continue
        i += 1
        frame = bytearray()
        while i < len(raw) and raw[i] != FEND:
            if raw[i] == FESC:
                i += 1
                if i >= len(raw):
                    break
                if raw[i] == TFEND:
                    frame.append(FEND)
                elif raw[i] == TFESC:
                    frame.append(FESC)
            else:
                frame.append(raw[i])
            i += 1
        i += 1
        if len(frame) > 1 and frame[0] == KISS_DATA_FRAME:
            frames.append(bytes(frame[1:]))
    return frames


# ===========================================================================
# Radio wrappers (thin layer over LoRaRF)
# ===========================================================================

class LoRaRFRadio:
    """
    Wraps LoRaRF SX126x or SX127x in the interface expected by the bridge:
      begin(), configure(rf_cfg), start_receive(callback), transmit(payload), close()
    """

    def __init__(self, profile: dict):
        self._profile = profile
        self._chip    = profile["chip"]
        self._lora    = None
        self._rx_callback  = None
        self._rx_thread    = None
        self._running      = False

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def begin(self):
        if not LORALIB_AVAILABLE:
            raise RuntimeError(
                "LoRaRF library not found. Install with: pip3 install LoRaRF"
            )

        p    = self._profile["pins"]
        spi  = self._profile["spi"]
        tcxo = self._profile.get("tcxo", {})

        if self._chip == "sx1262":
            self._lora = SX126x()
        elif self._chip in ("sx1276", "sx1278"):
            self._lora = SX127x()
        else:
            raise ValueError(f"Unsupported chip: {self._chip}")

        if self._chip in ("sx1276", "sx1278"):
            # SX127x: begin() handles setSpi/setPins internally — pass all
            # config directly so its internal calls use our values, not defaults.
            ok = self._lora.begin(
                spi["bus"],
                spi["device"],
                p["reset"],
                p.get("irq",   -1) or -1,
                p.get("tx_en", -1) or -1,
                p.get("rx_en", -1) or -1,
            )
            if not ok:
                raise RuntimeError(
                    f"LoRaRF begin() failed for profile '{self._profile.get('description')}' "
                    "— check SPI and GPIO wiring"
                )

        else:
            # SX126x: configure SPI and pins first, then call begin()
            self._lora.setSPI(spi["bus"], spi["device"],
                              spi.get("max_speed_hz", 2_000_000))
            self._lora.setPins(
                p["cs"],
                p["reset"],
                p.get("busy",  -1) or -1,
                p.get("irq",   -1) or -1,
                p.get("tx_en", -1) or -1,
                p.get("rx_en", -1) or -1,
            )
            # TCXO on DIO3 — SX1262 only
            if tcxo.get("enabled"):
                self._lora.setDio3TcxoCtrl(
                    tcxo.get("voltage", 1.8),
                    int(tcxo.get("delay_ms", 5) * 1000)   # µs
                )
            if not self._lora.begin():
                raise RuntimeError(
                    f"LoRaRF begin() failed for profile '{self._profile.get('description')}' "
                    "— check SPI and GPIO wiring"
                )

        log.info("Radio ready: %s", self._profile.get("description", self._chip))

    def close(self):
        self._running = False
        if self._lora:
            self._lora.end()

    # -----------------------------------------------------------------------
    # RF configuration
    # -----------------------------------------------------------------------

    def configure(self, rf: dict):
        freq_hz = int(rf["frequency_mhz"] * 1_000_000)
        bw_hz   = int(rf["bandwidth_khz"]  * 1_000)
        sf      = rf["spreading_factor"]
        cr      = rf["coding_rate"]         # denominator: 5 = CR4/5
        sw      = rf["sync_word"]
        dbm     = rf["tx_power_dbm"]
        preamble = rf["preamble_length"]
        implicit = rf["implicit_header"]
        crc      = rf["crc_enabled"]

        # Low Data Rate Optimisation is mandatory for SF11/SF12 + BW125
        ldro = (sf >= 11 and rf["bandwidth_khz"] <= 125)

        self._lora.setFrequency(freq_hz)

        # SX126x and SX127x share the same setLoRaModulation signature in LoRaRF
        self._lora.setLoRaModulation(sf, bw_hz, cr, ldro)

        # setLoRaPacket(headerType, preambleLength, payloadLength, crcType, invertIq)
        header_type = self._lora.HEADER_IMPLICIT if implicit else self._lora.HEADER_EXPLICIT
        self._lora.setLoRaPacket(header_type, preamble, 0xFF, crc, False)

        self._lora.setSyncWord(sw)

        # TX power
        if self._chip == "sx1262":
            self._lora.setTxPower(dbm, self._lora.TX_POWER_SX1262)
        else:
            # SX127x requires PA pin argument; RFM9x boards use PA_BOOST path
            self._lora.setTxPower(dbm, self._lora.TX_POWER_PA_BOOST)

        # Boosted LNA for best RX sensitivity
        # SX127x setRxGain takes (boost, level) — SX126x takes (gain,)
        if self._chip in ("sx1276", "sx1278"):
            self._lora.setRxGain(self._lora.RX_GAIN_BOOSTED, 0)
        else:
            self._lora.setRxGain(self._lora.RX_GAIN_BOOSTED)

        log.info(
            "RF configured: %.3f MHz  SF%d  BW%g kHz  CR4/%d  SW=0x%02X  %d dBm",
            rf["frequency_mhz"], sf, rf["bandwidth_khz"], cr, sw, dbm,
        )

    # -----------------------------------------------------------------------
    # Transmit
    # -----------------------------------------------------------------------

    def transmit(self, payload: bytes) -> bool:
        self._lora.beginPacket()
        self._lora.write(list(payload), len(payload))
        self._lora.endPacket()
        result = self._lora.wait(10_000)  # 10 s timeout in ms
        # SX127x wait() returns bool; SX126x returns a status code
        if self._chip in ("sx1276", "sx1278"):
            success = bool(result)
        else:
            success = (result == self._lora.STATUS_TX_DONE)
        if not success:
            log.warning("TX failed — status=%s", result)
            return False
        log.debug("TX done, %d bytes", len(payload))
        return True

    # -----------------------------------------------------------------------
    # Receive — runs in a background thread
    # -----------------------------------------------------------------------

    def start_receive(self, callback):
        self._rx_callback = callback
        self._running = True
        self._rx_thread = threading.Thread(
            target=self._rx_loop, name="lora-rx", daemon=True
        )
        self._rx_thread.start()
        log.info("RX thread started")

    def stop_receive(self):
        self._running = False
        if self._rx_thread:
            self._rx_thread.join(timeout=3)

    def _rx_loop(self):
        """
        LoRaRF receive loop.
        request() puts the radio into single-receive mode; wait() blocks until
        a packet arrives or times out.  We loop continuously for iGate use.
        """
        while self._running:
            # request() → single RX; timeout 0 = wait forever (SX126x)
            # SX127x request() / wait() return bool instead of status codes
            self._lora.request()
            result = self._lora.wait(0)

            if not self._running:
                break

            if self._chip in ("sx1276", "sx1278"):
                got_packet = bool(result)
            else:
                got_packet = (result == self._lora.STATUS_RX_DONE)

            if got_packet:
                length = self._lora.available()
                if length > 0:
                    payload = bytes(self._lora.read(length))
                    rssi = self._lora.packetRssi()
                    snr  = self._lora.snr()
                    log.info(
                        "RX %d bytes  RSSI=%d dBm  SNR=%.1f dB",
                        length, rssi, snr,
                    )
                    if self._rx_callback:
                        self._rx_callback(payload, rssi, snr)
            elif self._chip not in ("sx1276", "sx1278") and result == self._lora.STATUS_RX_TIMEOUT:
                pass   # normal — just loop back into request()
            else:
                log.debug("RX status: %s", result)


# ===========================================================================
# External KISS passthrough (TTGO / Heltec over UART)
# ===========================================================================

class ExternalKISSRadio:
    """
    Passthrough for hardware that already exposes KISS TNC over a serial
    port (e.g. TTGO LoRa32 running firmware with KISS output).
    The bridge relays bytes between the serial port and Direwolf.
    No LoRaRF dependency — just pyserial.
    """

    def __init__(self, profile: dict):
        self._port = profile["serial"]["port"]
        self._baud = profile["serial"]["baud"]
        self._ser  = None
        self._rx_callback = None
        self._running = False

    def begin(self):
        if not SERIAL_AVAILABLE:
            raise RuntimeError(
                "pyserial not found. Install with: pip3 install pyserial"
            )
        self._ser = pyserial.Serial(self._port, self._baud, timeout=0.1)
        log.info("ExternalKISS: connected to %s @ %d baud", self._port, self._baud)

    def configure(self, _rf):
        log.info("ExternalKISS: RF config handled by external MCU — skipping")

    def start_receive(self, callback):
        self._rx_callback = callback
        self._running = True
        t = threading.Thread(target=self._loop, name="ext-kiss-rx", daemon=True)
        t.start()

    def stop_receive(self):
        self._running = False

    def transmit(self, payload: bytes) -> bool:
        if self._ser and self._ser.is_open:
            self._ser.write(kiss_encode(payload))
            return True
        return False

    def close(self):
        self._running = False
        if self._ser:
            self._ser.close()

    def _loop(self):
        buf = b""
        while self._running:
            data = self._ser.read(256)
            if data:
                buf += data
                frames = kiss_decode(buf)
                for frame in frames:
                    if self._rx_callback:
                        self._rx_callback(frame, 0, 0.0)
                if frames:
                    buf = b""


# ===========================================================================
# Radio factory
# ===========================================================================

def build_radio(profile: dict):
    chip = profile.get("chip", "sx1262")
    if chip == "external_kiss":
        return ExternalKISSRadio(profile)
    elif chip in ("sx1262", "sx1276", "sx1278"):
        return LoRaRFRadio(profile)
    else:
        raise ValueError(f"Unknown chip type: '{chip}'")


# ===========================================================================
# KISS TNC server  (PTY or TCP)
# ===========================================================================

class KISSServer:
    """
    Exposes a KISS TNC endpoint that Direwolf connects to.

    mode "pty" — creates a pseudo-terminal; Direwolf uses SERIALKISS /dev/pts/N
    mode "tcp" — listens on a TCP port;    Direwolf uses KISSPORT <port>
    """

    def __init__(self, cfg: dict):
        self.mode      = cfg.get("mode", "pty")
        self.tcp_port  = cfg.get("tcp_port", 8001)
        self.pty_link  = cfg.get("pty_symlink", "/tmp/kiss_lora")
        self._clients  = []
        self._lock     = threading.Lock()
        self._master_fd = None

    def start(self):
        if self.mode == "pty":
            self._start_pty()
        else:
            self._start_tcp()

    # --- outbound callback (TX from Direwolf → radio) ----------------------
    # Overridden by the bridge after construction
    def on_outbound(self, payload: bytes):
        pass

    # --- push RX packet to Direwolf ----------------------------------------
    def send_to_direwolf(self, payload: bytes):
        frame = kiss_encode(payload)
        if self.mode == "pty" and self._master_fd is not None:
            try:
                os.write(self._master_fd, frame)
            except OSError as exc:
                log.warning("PTY write error: %s", exc)
        else:
            with self._lock:
                dead = []
                for client in self._clients:
                    try:
                        client.sendall(frame)
                    except OSError:
                        dead.append(client)
                for c in dead:
                    self._clients.remove(c)

    # -----------------------------------------------------------------------

    def _start_pty(self):
        master, slave = pty.openpty()
        self._master_fd = master
        slave_name = os.ttyname(slave)
        try:
            os.unlink(self.pty_link)
        except FileNotFoundError:
            pass
        os.symlink(slave_name, self.pty_link)
        log.info("PTY TNC ready: %s → %s", self.pty_link, slave_name)
        t = threading.Thread(target=self._pty_read_loop, daemon=True)
        t.start()

    def _pty_read_loop(self):
        buf = b""
        while True:
            try:
                r, _, _ = select.select([self._master_fd], [], [], 1.0)
                if r:
                    data = os.read(self._master_fd, 512)
                    buf += data
                    for frame in kiss_decode(buf):
                        self.on_outbound(frame)
                    buf = b""
            except OSError:
                break

    def _start_tcp(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.tcp_port))
        srv.listen(4)
        log.info("TCP KISS TNC listening on 127.0.0.1:%d", self.tcp_port)
        t = threading.Thread(target=self._accept_loop, args=(srv,), daemon=True)
        t.start()

    def _accept_loop(self, srv):
        while True:
            conn, addr = srv.accept()
            log.info("Direwolf connected from %s", addr)
            with self._lock:
                self._clients.append(conn)
            t = threading.Thread(
                target=self._client_read_loop, args=(conn,), daemon=True
            )
            t.start()

    def _client_read_loop(self, conn):
        buf = b""
        while True:
            try:
                data = conn.recv(512)
                if not data:
                    break
                buf += data
                for frame in kiss_decode(buf):
                    self.on_outbound(frame)
                buf = b""
            except OSError:
                break
        with self._lock:
            if conn in self._clients:
                self._clients.remove(conn)
        log.info("Direwolf disconnected")


# ===========================================================================
# Main bridge
# ===========================================================================

class LoRaKISSBridge:

    def __init__(self, profile_name: str, config_path: str):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        profiles = cfg.get("profiles", {})
        if profile_name not in profiles:
            raise KeyError(
                f"Profile '{profile_name}' not found. "
                f"Available profiles: {list(profiles.keys())}"
            )

        self._profile_name = profile_name
        self._profile      = profiles[profile_name]
        self._rf_cfg       = cfg.get("aprs_rf", {})
        self._kiss_cfg     = cfg.get("kiss_tnc", {})

        self._radio = build_radio(self._profile)
        self._tnc   = KISSServer(self._kiss_cfg)
        self._tnc.on_outbound = self._on_tx_request

        self._rx_count = 0
        self._tx_count = 0

    def start(self):
        self._radio.begin()
        self._radio.configure(self._rf_cfg)
        self._tnc.start()
        self._radio.start_receive(self._on_rx)

        log.info(
            "Bridge running  profile=%s  %.3f MHz  SF%d  BW%g kHz",
            self._profile_name,
            self._rf_cfg["frequency_mhz"],
            self._rf_cfg["spreading_factor"],
            self._rf_cfg["bandwidth_khz"],
        )

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        # Periodic stats on main thread
        while True:
            time.sleep(60)
            log.info("Stats — RX packets: %d  TX packets: %d",
                     self._rx_count, self._tx_count)

    def _on_rx(self, payload: bytes, rssi: int, snr: float):
        self._rx_count += 1
        log.debug("RX payload hex: %s", payload.hex())
        self._tnc.send_to_direwolf(payload)

    def _on_tx_request(self, payload: bytes):
        self._tx_count += 1
        log.debug("TX payload hex: %s", payload.hex())
        self._radio.transmit(payload)

    def _shutdown(self, *_):
        log.info("Shutting down…")
        self._radio.stop_receive()
        self._radio.close()
        try:
            os.unlink(self._kiss_cfg.get("pty_symlink", "/tmp/kiss_lora"))
        except FileNotFoundError:
            pass
        sys.exit(0)


# ===========================================================================
# Entry point
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="LoRa APRS → KISS TNC bridge for Direwolf  (uses LoRaRF library)"
    )
    parser.add_argument(
        "--profile", default="meshadv",
        help="Hardware profile name in hardware_profiles.yaml (default: meshadv)"
    )
    parser.add_argument(
        "--config", default="hardware_profiles.yaml",
        help="Path to hardware_profiles.yaml"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    bridge = LoRaKISSBridge(args.profile, args.config)
    bridge.start()


if __name__ == "__main__":
    main()
