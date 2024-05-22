# Python Logger Code Instructions

Here you can find instructions on how to use the Koheesio Logging Factory.

## Logging Factory

The `LoggingFactory` class is a factory for creating and configuring loggers. To use it, follow these steps:

1. Import the necessary modules:

    ```python
    from koheesio.logger import LoggingFactory
    ```

2. Initialize logging factory for koheesio modules:

    ```python
    factory = LoggingFactory(name="replace_koheesio_parent_name", env="local", logger_id="your_run_id")
    # Or use default 
    factory = LoggingFactory()
    # Or just specify log level for koheesio modules
    factory = LoggingFactory(level="DEBUG")
    ```

3. Create a logger by calling the `create_logger` method of the `LoggingFactory` class, you can inherit from koheesio logger:

    ```python
    logger = LoggingFactory.get_logger(name=factory.LOGGER_NAME)
   # Or for koheesio modules
    logger = LoggingFactory.get_logger(name=factory.LOGGER_NAME,inherit_from_koheesio=True)
    ```

4. You can now use the `logger` object to log messages:

    ```python
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")
    ```

5. (Optional) You can add additional handlers to the logger by calling the `add_handlers` method of the `LoggingFactory` class:

    ```python
    handlers = [
        ("your_handler_module.YourHandlerClass", {"level": "INFO"}),
        # Add more handlers if needed
    ]
    factory.add_handlers(handlers)
    ```

6. (Optional) You can create child loggers based on the parent logger by calling the `get_logger` method of the `LoggingFactory` class:

    ```python
    child_logger = factory.get_logger(name="your_child_logger_name")
    ```

7. (Optional) Get an independent logger without inheritance

    If you need an independent logger without inheriting from the `LoggingFactory` logger, you can use the `get_logger` method:

    ```python
    your_logger = factory.get_logger(name="your_logger_name", inherit=False)
    ```

   By setting `inherit` to `False`, you will obtain a logger that is not tied to the `LoggingFactory` logger hierarchy, only format of message will be the same, but you can also change it. This allows you to have an independent logger with its own configuration.
   You can use the `your_logger` object to log messages:

    ```python
    your_logger.debug("Debug message")
    your_logger.info("Info message")
    your_logger.warning("Warning message")
    your_logger.error("Error message")
    your_logger.critical("Critical message")
    ```

8. (Optional) You can use Masked types to masked secrets/tokens/passwords in output. The Masked types are special types provided by the koheesio library to handle sensitive data
    that should not be logged or printed in plain text. They are used to wrap sensitive data and override their string representation to prevent accidental exposure of the data.Here are some examples of how to use Masked types:

    ```python
    import logging
    from koheesio.logger import MaskedString, MaskedInt, MaskedFloat, MaskedDict

    # Set up logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    # Using MaskedString
    masked_string = MaskedString("my secret string")
    logger.info(masked_string)  # This will not log the actual string

    # Using MaskedInt
    masked_int = MaskedInt(12345)
    logger.info(masked_int)  # This will not log the actual integer

    # Using MaskedFloat
    masked_float = MaskedFloat(3.14159)
    logger.info(masked_float)  # This will not log the actual float

    # Using MaskedDict
    masked_dict = MaskedDict({"key": "value"})
    logger.info(masked_dict)  # This will not log the actual dictionary
    ```

Please make sure to replace "your_logger_name", "your_run_id", "your_handler_module.YourHandlerClass", "your_child_logger_name", and other placeholders with your own values according to your application's requirements.

By following these steps, you can obtain an independent logger without inheriting from the `LoggingFactory` logger. This allows you to customize the logger configuration and use it separately in your code.

**Note:** Ensure that you have imported the necessary modules, instantiated the `LoggingFactory` class, and customized the logger name and other parameters according to your application's requirements.

## Example

```python
import logging

# Step 2: Instantiate the LoggingFactory class
factory = LoggingFactory(env="local")

# Step 3: Create an independent logger with a custom log level
your_logger = factory.get_logger("your_logger", inherit_from_koheesio=False)
your_logger.setLevel(logging.DEBUG)

# Step 4: Create a logger using the create_logger method from LoggingFactory with a different log level
factory_logger = LoggingFactory(level="WARNING").get_logger(name=factory.LOGGER_NAME)

# Step 5: Create a child logger with a debug level
child_logger = factory.get_logger(name="child")
child_logger.setLevel(logging.DEBUG)

child2_logger = factory.get_logger(name="child2")
child2_logger.setLevel(logging.INFO)

# Step 6: Log messages at different levels for both loggers
your_logger.debug("Debug message")  # This message will be displayed
your_logger.info("Info message")  # This message will be displayed
your_logger.warning("Warning message")  # This message will be displayed
your_logger.error("Error message")  # This message will be displayed
your_logger.critical("Critical message")  # This message will be displayed

factory_logger.debug("Debug message")  # This message will not be displayed
factory_logger.info("Info message")  # This message will not be displayed
factory_logger.warning("Warning message")  # This message will be displayed
factory_logger.error("Error message")  # This message will be displayed
factory_logger.critical("Critical message")  # This message will be displayed

child_logger.debug("Debug message")  # This message will be displayed
child_logger.info("Info message")  # This message will be displayed
child_logger.warning("Warning message")  # This message will be displayed
child_logger.error("Error message")  # This message will be displayed
child_logger.critical("Critical message")  # This message will be displayed

child2_logger.debug("Debug message")  # This message will be displayed
child2_logger.info("Info message")  # This message will be displayed
child2_logger.warning("Warning message")  # This message will be displayed
child2_logger.error("Error message")  # This message will be displayed
child2_logger.critical("Critical message")  # This message will be displayed
```

Output:

```sh
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [DEBUG] [your_logger] {__init__.py:<module>:118} - Debug message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [INFO] [your_logger] {__init__.py:<module>:119} - Info message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [WARNING] [your_logger] {__init__.py:<module>:120} - Warning message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [ERROR] [your_logger] {__init__.py:<module>:121} - Error message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [CRITICAL] [your_logger] {__init__.py:<module>:122} - Critical message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [WARNING] [koheesio] {__init__.py:<module>:126} - Warning message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [ERROR] [koheesio] {__init__.py:<module>:127} - Error message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [CRITICAL] [koheesio] {__init__.py:<module>:128} - Critical message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [DEBUG] [koheesio.child] {__init__.py:<module>:130} - Debug message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [INFO] [koheesio.child] {__init__.py:<module>:131} - Info message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [WARNING] [koheesio.child] {__init__.py:<module>:132} - Warning message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [ERROR] [koheesio.child] {__init__.py:<module>:133} - Error message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [CRITICAL] [koheesio.child] {__init__.py:<module>:134} - Critical message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [INFO] [koheesio.child2] {__init__.py:<module>:137} - Info message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [WARNING] [koheesio.child2] {__init__.py:<module>:138} - Warning message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [ERROR] [koheesio.child2] {__init__.py:<module>:139} - Error message
[a7d79f7a-f16f-4d2a-8430-1134830f61be] [2023-06-26 20:29:39,267] [CRITICAL] [koheesio.child2] {__init__.py:<module>:140} - Critical message
```

## LoggerIDFilter Class

The `LoggerIDFilter` class is a filter that injects `run_id` information into the log. To use it, follow these steps:

1. Import the necessary modules:

    ```python
    import logging
    ```

2. Create an instance of the `LoggerIDFilter` class:

    ```python
    logger_filter = LoggerIDFilter()
    ```

3. Set the `LOGGER_ID` attribute of the `LoggerIDFilter` class to the desired run ID:

    ```python
    LoggerIDFilter.LOGGER_ID = "your_run_id"
    ```

4. Add the `logger_filter` to your logger or handler:

    ```python
    logger = logging.getLogger("your_logger_name")
    logger.addFilter(logger_filter)
    ```

## LoggingFactory Set Up (Optional)

1. Import the `LoggingFactory` class in your application code.

2. Set the value for the `LOGGER_FILTER` variable:
   - If you want to assign a specific `logging.Filter` instance, replace `None` with your desired filter instance.
   - If you want to keep the default value of `None`, leave it unchanged.

3. Set the value for the `LOGGER_LEVEL` variable:
   - If you want to use the value from the `"KOHEESIO_LOGGING_LEVEL"` environment variable, leave the code as is.
   - If you want to use a different environment variable or a specific default value, modify the code accordingly.

7. Set the value for the `LOGGER_ENV` variable:
   - Replace `"local"` with your desired environment name.

8. Set the value for the `LOGGER_FORMAT` variable:
   - If you want to customize the log message format, modify the value within the double quotes.
   - The format should follow the desired log message format pattern.

9. Set the value for the `LOGGER_FORMATTER` variable:
   - If you want to assign a specific `Formatter` instance, replace `Formatter(LOGGER_FORMAT)` with your desired formatter instance.
   - If you want to keep the default formatter with the defined log message format, leave it unchanged.

10. Set the value for the `CONSOLE_HANDLER` variable:
    - If you want to assign a specific `logging.Handler` instance, replace `None` with your desired handler instance.
    - If you want to keep the default value of `None`, leave it unchanged.

11. Set the value for the `ENV` variable:
    - Replace `None` with your desired environment value if applicable.
    - If you don't need to set this variable, leave it as `None`.

12. Save the changes to the file.
