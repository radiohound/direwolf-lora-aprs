/*------------------------------------------------------------------
 *
 * Module:      gpio_common.c
 *
 * Purpose:   	The libgpiod API changed drastically between v1 and v2, as first seen
 *		in Debian 13 Trixie.
 *		It is not possible to have the same application code work with both.
 *		It is necessary to have two sets of code and conditional compilation.
 *
 * Environment:	Preprocessor symbols set by build system:
 *		USE_GPIOD		to include the gpiod code
 *		LIBGPIOD_VERSION	e.g. 1.2.3
 *		LIBGPIOD_VERSION_MAJOR	e.g. 1
 *		LIBGPIOD_VERSION_MINOR	e.g. 2
 *
 * Description:	Eventually we would like to put all of the version differences
 *		in here to avoid cluttering ptt.c more than it is already.
 *		Currently this is all version 2 code which has no hope of
 *		building with version 1 library.
 *
 *---------------------------------------------------------------*/

#if USE_GPIOD				// Skip all of this if no GPIOD libary present

#if LIBGPIOD_VERSION_MAJOR >= 2		// Someday we might have v1 and v2 sections

#include "direwolf.h"

#include <gpiod.h>
#include <stdio.h>
#include <unistd.h>

#include <errno.h>
#include <string.h>

#include "textcolor.h"

#include "gpio_common.h"

#define GPIO_MAX_LINES 32
#define GPIO_CONSUMER "DIREWOLF"

//Types
typedef struct gpio_common {
  struct gpiod_line_request *request;
  unsigned int offset;
  bool used;
} gpio_common_t;

// local variables

static gpio_common_t gpio[GPIO_MAX_LINES];


// Function implementations

void gpio_common_init(void) {
  text_color_set(DW_COLOR_DEBUG);
  dw_printf("Initializing GPIO common structure\n");
  for (gpio_num_t i = 0; i < GPIO_MAX_LINES; i++) {
    gpio[i].used = false;
  }
}


gpio_num_t gpio_common_open_line(const char *chip_name, unsigned int line, bool active_low) {
  gpio_num_t gpio_num;
  int ret;

  struct gpiod_request_config *req_cfg = NULL;
	struct gpiod_line_settings *settings;
	struct gpiod_line_config *line_cfg;
	struct gpiod_chip *chip;

  gpio_num = GPIO_COMMON_UNKNOWN;

  if (chip_name == NULL) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("No chip name supplied.\n");
    goto out;
  }

  text_color_set(DW_COLOR_DEBUG);
  dw_printf("Opening GPIO line %d on chip %s\n", line, chip_name);

  // Get a free slot
  for (gpio_num_t i = 0; i < GPIO_MAX_LINES; i++) {
    if (gpio[i].used == false) {
      gpio_num = i;
      break;
    }
  }

  if (gpio_num == GPIO_COMMON_UNKNOWN) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("Too many GPIOs open.\n");
    goto out;
  }

  chip = gpiod_chip_open(chip_name);

  if (chip == NULL) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("Failed to open GPIO chip %s\n", chip_name);
    gpio_num = GPIO_COMMON_UNKNOWN;
    goto out;
  }

  settings = gpiod_line_settings_new();

  if (settings == NULL) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("Unable to allocate memory for line settings \n");
    gpio_num = GPIO_COMMON_UNKNOWN;
    goto close_chip;
  }

  gpiod_line_settings_set_direction(settings,
					  GPIOD_LINE_DIRECTION_OUTPUT);
	gpiod_line_settings_set_output_value(settings, 0);
  gpiod_line_settings_set_active_low(settings, active_low);

  line_cfg = gpiod_line_config_new();

  if (!line_cfg) {
    gpio_num = GPIO_COMMON_UNKNOWN;
    goto free_settings;
  }

  ret = gpiod_line_config_add_line_settings(line_cfg, &line, 1,
						  settings);
  if (ret < 0) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("Failed to add line settings\n");
    gpio_num = GPIO_COMMON_UNKNOWN;
    goto free_line_config;
  }

  req_cfg = gpiod_request_config_new();
	if (!req_cfg) {
		goto free_line_config;
  }

  gpiod_request_config_set_consumer(req_cfg, GPIO_CONSUMER);

  gpio[gpio_num].request = gpiod_chip_request_lines(chip, req_cfg, line_cfg);

  if (gpio[gpio_num].request == NULL) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("Failed to request GPIO line %d\n", gpio_num);
    gpio_num = GPIO_COMMON_UNKNOWN;
    goto free_line_config;
  } else {
    gpio[gpio_num].used = true;
    gpio[gpio_num].offset = line;
  }

free_line_config:
	gpiod_line_config_free(line_cfg);

free_settings:
	gpiod_line_settings_free(settings);

close_chip:
	gpiod_chip_close(chip);


out:

  return gpio_num;
}


int gpio_common_release_line(gpio_num_t gpio_num) {
  if (gpio_num >= GPIO_MAX_LINES) {
    return GPIO_COMMON_ERR;
  }

  if (gpio[gpio_num].request != NULL) {
    gpiod_line_request_release(gpio[gpio_num].request);
    gpio[gpio_num].request = NULL;
  }

  return 0;
}


int gpio_common_set(gpio_num_t gpio_num, bool val) {
  if (gpio_num >= GPIO_MAX_LINES || gpio[gpio_num].request == NULL) {
    return GPIO_COMMON_ERR;
  }
  uint16_t gpiod_val;

  if (val) {
    gpiod_val = GPIOD_LINE_VALUE_ACTIVE;
  } else {
    gpiod_val = GPIOD_LINE_VALUE_INACTIVE;
  }


  int ret = gpiod_line_request_set_value(gpio[gpio_num].request, gpio[gpio_num].offset, gpiod_val);
  if (ret < 0) {
    text_color_set(DW_COLOR_ERROR);
    dw_printf("Error setting line\n");
    return GPIO_COMMON_ERR;
  }
  return 0;
}


int gpio_common_close(void) {

  for (gpio_num_t i = 0; i < GPIO_MAX_LINES; i++) {
    gpio_common_release_line(i);
  }

  return 0;
}

#endif		// LIBGPIOD_VERSION_MAJOR >= 2

#endif		// USE_GPIOD

// end gpio_common.c
