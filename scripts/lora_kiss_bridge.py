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
import queue
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
# Force-free any GPIO pins left claimed by a previous crashed run.
# RPi.GPIO.cleanup() only releases pins owned by *this* process; a crashed
# prior run leaves lgpio chip handles open.  We free them at the lgpio level
# before RPi.GPIO tries to claim them, avoiding 'GPIO busy' on restart.
# ---------------------------------------------------------------------------
def _force_free_gpio_pins(pins):
    """Release pins via lgpio directly, ignoring errors if already free."""
    try:
        import lgpio as _lgpio  # type: ignore[import]
        _h = _lgpio.gpiochip_open(0)
        for _pin in pins:
            if _pin is not None and _pin >= 0:
                try:
                    _lgpio.gpio_free(_h, _pin)
                except Exception:
                    pass
        _lgpio.gpiochip_close(_h)
    except Exception:
        pass

# Pre-free all pins any profile might use so a crashed prior run can't block us
_force_free_gpio_pins([4, 5, 6, 7, 8, 12, 13, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27])

# ---------------------------------------------------------------------------
# Patch rpi-lgpio 0.6 bug: setup(pin, OUT) calls gpio_read() on an unclaimed
# pin, raising 'GPIO not allocated'.  Providing initial=0 skips that read.
# ---------------------------------------------------------------------------
try:
    import RPi.GPIO as _GPIO  # type: ignore[import]
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
# AX.25 ↔ TNC2 conversion
#
# LoRa APRS transmits plain TNC2 text over the air:
#   K6ATV-12>APRS,WIDE1-1:!3700.36NR12134.08W&LoRa iGate
#
# Direwolf communicates in binary AX.25 via KISS.  These helpers convert
# between the two formats so the bridge is compatible with the wider
# LoRa APRS ecosystem (OE5BPA iGate, RadioLib devices, etc.).
# ===========================================================================

def _ax25_decode_addr(raw7: bytes):
    """Decode one 7-byte AX.25 address field.
    Returns (callsign_with_ssid, h_bit, last_flag)."""
    callsign = "".join(chr(b >> 1) for b in raw7[:6]).rstrip()
    ssid_byte = raw7[6]
    ssid  = (ssid_byte >> 1) & 0x0F
    h_bit = bool(ssid_byte & 0x80)
    last  = bool(ssid_byte & 0x01)
    full  = f"{callsign}-{ssid}" if ssid else callsign
    return full, h_bit, last


def _ax25_encode_addr(addr_str: str, last: bool = False) -> bytes:
    """Encode a callsign[-SSID][*] string into a 7-byte AX.25 address field."""
    h_bit    = addr_str.endswith("*")
    addr_str = addr_str.rstrip("*")
    if "-" in addr_str:
        call, ssid_s = addr_str.rsplit("-", 1)
        ssid = int(ssid_s) if ssid_s.isdigit() else 0
    else:
        call, ssid = addr_str, 0
    call      = call.upper().ljust(6)[:6]
    result    = bytearray(7)
    for i, c in enumerate(call):
        result[i] = ord(c) << 1
    ssid_byte = 0x60 | ((ssid & 0x0F) << 1)
    if last:
        ssid_byte |= 0x01
    if h_bit:
        ssid_byte |= 0x80
    result[6] = ssid_byte
    return bytes(result)


def ax25_to_tnc2(frame: bytes):
    """Convert a binary AX.25 UI frame to a TNC2 string, or None on error."""
    i, addresses = 0, []
    while True:
        if i + 7 > len(frame):
            return None
        full, h_bit, last = _ax25_decode_addr(frame[i:i + 7])
        addresses.append(full + ("*" if h_bit else ""))
        i += 7
        if last:
            break
        if len(addresses) > 8:
            return None
    if i + 2 > len(frame) or len(addresses) < 2:
        return None
    ctrl, pid = frame[i], frame[i + 1]
    if ctrl != 0x03 or pid != 0xF0:
        return None
    dst, src = addresses[0], addresses[1]
    path     = addresses[2:]
    info     = frame[i + 2:].decode("ascii", errors="replace")
    header   = f"{src}>{dst}" + (("," + ",".join(path)) if path else "")
    return f"{header}:{info}"


def tnc2_to_ax25(tnc2: str):
    """Convert a TNC2 string to a binary AX.25 UI frame, or None on error."""
    try:
        colon = tnc2.index(":")
    except ValueError:
        return None
    header, info = tnc2[:colon], tnc2[colon + 1:]
    try:
        gt = header.index(">")
    except ValueError:
        return None
    src   = header[:gt]
    parts = header[gt + 1:].split(",")
    dst, path = parts[0], parts[1:]
    addrs = [dst, src] + path
    frame = bytearray()
    for idx, addr in enumerate(addrs):
        frame += _ax25_encode_addr(addr, last=(idx == len(addrs) - 1))
    frame += bytes([0x03, 0xF0])
    frame += info.encode("ascii", errors="replace")
    return bytes(frame)


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
        # TX requests from any thread are posted here; the single RX/TX loop
        # drains the queue between receive windows so the radio is never
        # accessed from two threads simultaneously.
        self._tx_queue = queue.Queue()
        # SX127x wait() timeout is in seconds; SX126x is in milliseconds.
        # Pre-compute the right values so callers don't need to know.
        if self._chip in ("sx1276", "sx1278"):
            self._wait_rx_timeout  = 1      # 1 s receive window
            self._wait_tx_timeout  = 10     # 10 s TX done timeout
        else:
            self._wait_rx_timeout  = 1000   # 1000 ms receive window
            self._wait_tx_timeout  = 10_000 # 10 000 ms TX done timeout

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
            self._lora.setSpi(spi["bus"], spi["device"],
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
        # Post the TX request to the queue so the radio loop executes it
        # between RX windows — no two threads ever touch the radio at once.
        done_event  = threading.Event()
        result_box  = []            # mutable container for the bool result
        self._tx_queue.put((payload, result_box, done_event))
        done_event.wait(timeout=30) # block caller until TX completes (or 30 s)
        return bool(result_box and result_box[0])

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

    def _do_transmit(self, payload: bytes, result_box: list, done_event: threading.Event):
        """Execute a TX request synchronously (called from the radio loop thread)."""
        self._lora.beginPacket()
        self._lora.write(list(payload), len(payload))
        self._lora.endPacket()
        result = self._lora.wait(self._wait_tx_timeout)
        if self._chip in ("sx1276", "sx1278"):
            success = bool(result)
        else:
            success = (result == self._lora.STATUS_TX_DONE)
        if not success:
            log.warning("TX failed — status=%s", result)
        else:
            log.info("TX done, %d bytes", len(payload))
        result_box.append(success)
        done_event.set()

    def _rx_loop(self):
        """
        Single radio thread — handles both RX and TX.

        Before each receive window, drains the TX queue so transmit() requests
        from other threads are serviced promptly.  All radio access stays on
        this one thread; no locks needed.
        """
        while self._running:
            # --- service any pending TX requests first -------------------
            while True:
                try:
                    payload, result_box, done_event = self._tx_queue.get_nowait()
                    self._do_transmit(payload, result_box, done_event)
                except queue.Empty:
                    break

            if not self._running:
                break

            # --- single receive window (1 s timeout so TX isn't delayed) -
            self._lora.request()
            result = self._lora.wait(self._wait_rx_timeout)  # returns to check TX queue

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
                        try:
                            self._rx_callback(payload, rssi, snr)
                        except Exception as exc:
                            log.warning("RX callback error (packet dropped): %s", exc)
            # timeout (False for SX127x, STATUS_RX_TIMEOUT for SX126x) is normal


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

    mode "pty"        — creates a pseudo-terminal; Direwolf uses SERIALKISS /dev/pts/N
    mode "tcp"        — listens on a TCP port;    Direwolf uses KISSPORT <port>
    mode "tcp_client" — connects TO Direwolf's KISSPORT as a client (recommended)
    """

    def __init__(self, cfg: dict):
        self.mode      = cfg.get("mode", "tcp_client")
        self.tcp_port  = cfg.get("tcp_port", 8001)
        self.tcp_host  = cfg.get("tcp_host", "127.0.0.1")
        self.pty_link  = cfg.get("pty_symlink", "/tmp/kiss_lora")
        self._clients  = []
        self._lock     = threading.Lock()
        self._master_fd = None

    def start(self):
        if self.mode == "none":
            return
        elif self.mode == "pty":
            self._start_pty()
        elif self.mode == "tcp_client":
            self._start_tcp_client()
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
        import tty as _tty
        master, slave = pty.openpty()
        self._master_fd = master
        slave_name = os.ttyname(slave)
        # Put the slave in raw mode so binary KISS frames pass through
        # immediately without line-buffering or special-character processing.
        _tty.setraw(slave)
        try:
            os.unlink(self.pty_link)
        except FileNotFoundError:
            pass
        os.symlink(slave_name, self.pty_link)
        log.info("PTY TNC ready: %s → %s", self.pty_link, slave_name)
        t = threading.Thread(target=self._pty_read_loop, daemon=True)
        t.start()

    def _pty_read_loop(self):
        import errno as _errno
        buf = b""
        log.debug("PTY read loop started, master_fd=%s", self._master_fd)
        while True:
            try:
                r, _, _ = select.select([self._master_fd], [], [], 1.0)
                if not r:
                    continue
                data = os.read(self._master_fd, 512)
                log.debug("PTY read %d bytes: %s", len(data), data.hex())
                buf += data
                frames = kiss_decode(buf)
                log.debug("kiss_decode found %d frame(s)", len(frames))
                for frame in frames:
                    self.on_outbound(frame)
                buf = b""
            except OSError as exc:
                if exc.errno == _errno.EIO:
                    buf = b""
                    time.sleep(0.5)
                else:
                    break

    def _start_tcp_client(self):
        """Connect to Direwolf's KISSPORT as a TCP client (retries until connected)."""
        t = threading.Thread(target=self._tcp_client_loop, daemon=True)
        t.start()

    def _tcp_client_loop(self):
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.tcp_host, self.tcp_port))
                log.info("Connected to Direwolf KISS on %s:%d", self.tcp_host, self.tcp_port)
                with self._lock:
                    self._clients = [sock]
                self._client_read_loop(sock)
            except OSError as exc:
                log.warning("Direwolf KISS connect failed (%s) — retrying in 5 s", exc)
                with self._lock:
                    self._clients = []
                time.sleep(5)

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
# APRS-IS gateway
# ===========================================================================

def _add_igate_path(tnc2: str, callsign: str) -> str:
    """Insert ,qAR,{callsign} into the TNC2 path so APRS-IS records the igate."""
    try:
        colon = tnc2.index(":")
        return f"{tnc2[:colon]},qAR,{callsign}{tnc2[colon:]}"
    except ValueError:
        return tnc2


class APRSISGateway:
    """Direct APRS-IS TCP connection — uploads packets, sends keepalives,
    and delivers incoming IS packets to on_is_packet callback."""

    def __init__(self, cfg: dict):
        self._server   = cfg.get("server", "rotate.aprs2.net")
        self._port     = int(cfg.get("port", 14580))
        self._callsign = cfg["callsign"]
        self._passcode = str(cfg["passcode"])
        self._filter   = cfg.get("filter", "")
        self._sock     = None
        self._lock     = threading.Lock()
        self.on_is_packet = None   # callback(tnc2: str) for IS→RF gating

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True).start()
        threading.Thread(target=self._keepalive_loop, daemon=True).start()

    def _connect_loop(self):
        while True:
            try:
                self._run()
            except Exception as exc:
                log.warning("APRS-IS disconnected: %s — reconnect in 15 s", exc)
            with self._lock:
                if self._sock:
                    try:
                        self._sock.close()
                    except OSError:
                        pass
                    self._sock = None
            time.sleep(15)

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(60)
        sock.connect((self._server, self._port))
        rf = sock.makefile("r", encoding="ascii", errors="replace")
        log.info("APRS-IS: %s", rf.readline().strip())
        login = f"user {self._callsign} pass {self._passcode} vers lora_bridge 1.0"
        if self._filter:
            login += f" filter {self._filter}"
        sock.sendall((login + "\r\n").encode("ascii"))
        log.info("APRS-IS login: %s", rf.readline().strip())
        with self._lock:
            self._sock = sock
        for line in rf:
            line = line.strip()
            if line and not line.startswith("#") and self.on_is_packet:
                self.on_is_packet(line)

    def _keepalive_loop(self):
        while True:
            time.sleep(25)
            with self._lock:
                if self._sock:
                    try:
                        self._sock.sendall(b"# keepalive\r\n")
                    except OSError:
                        pass

    def upload(self, tnc2: str):
        with self._lock:
            if not self._sock:
                log.warning("APRS-IS not connected, dropping: %s", tnc2)
                return
            try:
                self._sock.sendall((tnc2.strip() + "\r\n").encode("ascii", errors="replace"))
                log.info("IS ← RF: %s", tnc2)
            except OSError as exc:
                log.warning("APRS-IS upload error: %s", exc)
                self._sock = None


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
        self._aprs_cfg     = cfg.get("aprs_is", {})

        self._igtx_cfg = self._aprs_cfg.get("igtx", {})

        self._radio = build_radio(self._profile)
        self._tnc   = KISSServer(self._kiss_cfg)
        self._tnc.on_outbound = self._on_tx_request
        self._aprs  = APRSISGateway(self._aprs_cfg) if self._aprs_cfg else None
        if self._aprs:
            self._aprs.on_is_packet = self._on_is_packet

        self._rx_count  = 0
        self._tx_count  = 0
        # IS→RF state
        self._heard_rf  = {}   # callsign → last heard time (float)
        self._igtx_times = []  # timestamps of recent IS→RF transmissions

    def start(self):
        self._radio.begin()
        self._radio.configure(self._rf_cfg)
        self._tnc.start()
        if self._aprs:
            self._aprs.start()
            if self._aprs_cfg.get("beacon"):
                threading.Thread(target=self._beacon_loop, daemon=True).start()
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

    def _beacon_loop(self):
        bcfg    = self._aprs_cfg.get("beacon", {})
        call    = self._aprs_cfg.get("callsign", "N0CALL")
        lat     = bcfg.get("lat",     "0000.00N")
        lon     = bcfg.get("lon",     "00000.00W")
        overlay = bcfg.get("overlay", "R")
        symbol  = bcfg.get("symbol",  "&")
        comment = bcfg.get("comment", "LoRa APRS iGate")
        delay   = bcfg.get("delay_s",    30)
        every   = bcfg.get("interval_s", 1800)

        info      = f"!{lat}{overlay}{lon}{symbol}{comment}"
        beacon_rf = f"{call}>APZLOR:{info}"
        beacon_is = f"{call}>APZLOR,TCPIP*:{info}"

        time.sleep(delay)
        while True:
            log.info("Beacon → LoRa RF: %s", beacon_rf)
            self._radio.transmit(beacon_rf.encode("ascii"))
            if self._aprs:
                self._aprs.upload(beacon_is)
            time.sleep(every)

    def _on_rx(self, payload: bytes, rssi: int, snr: float):
        self._rx_count += 1
        try:
            tnc2 = payload.decode("ascii", errors="replace").strip()
        except Exception:
            log.warning("RX: cannot decode payload as text, dropping")
            return
        log.info("RX ← LoRa: %s  (RSSI=%d dBm  SNR=%.1f dB)", tnc2, rssi, snr)

        # Track source callsign for IS→RF heard-station filtering
        try:
            src = tnc2[:tnc2.index(">")].strip().upper()
            self._heard_rf[src] = time.time()
        except ValueError:
            pass

        if self._aprs:
            self._aprs.upload(_add_igate_path(tnc2, self._aprs_cfg.get("callsign", "N0CALL")))
        else:
            ax25 = tnc2_to_ax25(tnc2)
            if ax25 is None:
                log.warning("RX: TNC2→AX.25 failed for: %s", tnc2)
                return
            self._tnc.send_to_direwolf(ax25)

    def _on_is_packet(self, tnc2: str):
        """Gate an APRS-IS packet to LoRa RF if it meets igtx criteria."""
        if not self._igtx_cfg.get("enabled", True):
            return

        # Only gate message packets (info starts with ':')
        try:
            colon = tnc2.index(":")
            info = tnc2[colon + 1:]
        except ValueError:
            return
        if not info.startswith(":"):
            return

        # Extract addressee (9-char padded field after first ':')
        addressee = info[1:10].strip().upper()

        # Only gate to stations heard over LoRa RF recently
        heard_ttl = self._igtx_cfg.get("heard_ttl_s", 1800)
        last_heard = self._heard_rf.get(addressee, 0)
        if time.time() - last_heard > heard_ttl:
            log.debug("IS→RF: %s not heard recently, skipping", addressee)
            return

        # Rate limiting: max N packets per period
        max_pkts = self._igtx_cfg.get("max_packets", 3)
        period   = self._igtx_cfg.get("period_s", 600)
        now = time.time()
        self._igtx_times = [t for t in self._igtx_times if now - t < period]
        if len(self._igtx_times) >= max_pkts:
            log.warning("IS→RF: rate limit reached (%d/%ds), dropping", max_pkts, period)
            return

        self._igtx_times.append(now)
        self._tx_count += 1
        log.info("IS→RF → LoRa: %s", tnc2)
        self._radio.transmit(tnc2.encode("ascii", errors="replace"))

    def _on_tx_request(self, payload: bytes):
        self._tx_count += 1
        tnc2 = ax25_to_tnc2(payload)
        if tnc2 is None:
            log.warning("TX: AX.25 decode failed (%d bytes), dropping", len(payload))
            return
        log.info("TX → LoRa: %s", tnc2)
        self._radio.transmit(tnc2.encode("ascii", errors="replace"))

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
        "--config",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "hardware_profiles.yaml"),
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
