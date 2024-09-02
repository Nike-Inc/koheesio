# Learn Koheesio

Koheesio is designed to simplify the development of data engineering pipelines. It provides a structured way to define
and execute data processing tasks, making it easier to build, test, and maintain complex data workflows.

## Core Concepts

Koheesio is built around several core concepts:

- **Step**: The fundamental unit of work in Koheesio. It represents a single operation in a data pipeline, taking in 
  inputs and producing outputs.  
  > See the [Step](../reference/concepts/step.md) documentation for more information.
- **Context**: A configuration class used to set up the environment for a Task. It can be used to share variables across
  tasks and adapt the behavior of a Task based on its environment.  
  > See the [Context](../reference/concepts/context.md) documentation for more information.
- **Logger**: A class for logging messages at different levels.  
  > See the [Logger](../reference/concepts/logger.md) documentation for more information.

The Logger and Context classes provide support, enabling detailed logging of the pipeline's execution and customization 
of the pipeline's behavior based on the environment, respectively.


## Implementations

In the context of Koheesio, an implementation refers to a specific way of executing Steps, the fundamental units of work
in Koheesio. Each implementation uses a different technology or approach to process data along with its own set of 
Steps, designed to work with the specific technology or approach used by the implementation. 

For example, the Spark implementation includes Steps for reading data from a Spark DataFrame, transforming the data 
using Spark operations, and writing the data to a Spark-supported destination.

Currently, Koheesio supports two implementations: Spark, and AsyncIO.

### Spark 
_Requires:_ Apache Spark (pyspark)
_Installation:_ `pip install koheesio[spark]`
_Module:_ `koheesio.spark`    

This implementation uses Apache Spark, a powerful open-source unified analytics engine for large-scale data processing. 

Steps that use this implementation can leverage Spark's capabilities for distributed data processing, making it suitable
for handling large volumes of data. The Spark implementation includes the following types of Steps:  

- **Reader**:  
  `from koheesio.spark.readers import Reader`  
  A type of Step that reads data from a source and stores the result (to make it available for subsequent steps).  
  For more information, see the [Reader](../reference/spark/readers.md) documentation.

- **Writer**:  
  `from koheesio.spark.writers import Writer`    
  This controls how data is written to the output in both batch and streaming contexts.  
  For more information, see the [Writer](../reference/spark/writers.md) documentation.

- **Transformation**:  
  `from koheesio.spark.transformations import Transformation`    
  A type of Step that takes a DataFrame as input and returns a DataFrame as output.
  For more information, see the [Transformation](../reference/spark/transformations.md) documentation.

In any given pipeline, you can expect to use Readers, Writers, and Transformations to express the ETL logic. Readers are
responsible for extracting data from various sources, such as databases, files, or APIs. Transformations then process this 
data, performing operations like filtering, aggregation, or conversion. Finally, Writers handle the loading of the
transformed data to the desired destination, which could be a database, a file, or a data stream. 

### Async
_Module:_ `koheesio.asyncio`

This implementation uses Python's asyncio library for writing single-threaded concurrent code using coroutines, 
multiplexing I/O access over sockets and other resources, running network clients and servers, and other related 
primitives. Steps that use this implementation can perform data processing tasks asynchronously, which can be beneficial
for IO-bound tasks.  


## Best Practices

Here are some best practices for using Koheesio:

1. **Use Context**: The `Context` class in Koheesio is designed to behave like a dictionary, but with added features. 
     It's a good practice to use `Context` to customize the behavior of a task. This allows you to share variables
     across tasks and adapt the behavior of a task based on its environment; for example, by changing the source or
     target of the data between development and production environments.

2. **Modular Design**: Each step in the pipeline (reading, transformation, writing) should be encapsulated in its own
     class, making the code easier to understand and maintain. This also promotes re-usability as steps can be reused
     across different tasks.

3. **Error Handling**: Koheesio provides a consistent way to handle errors and exceptions in data processing tasks. Make
     sure to leverage this feature to make your pipelines robust and fault-tolerant.

4. **Logging**: Use the built-in logging feature in Koheesio to log information and errors in data processing tasks.
     This can be very helpful for debugging and monitoring the pipeline. Koheesio sets the log level to `WARNING` by
     default, but you can change it to `INFO` or `DEBUG` as needed.

5. **Testing**: Each step can be tested independently, making it easier to write unit tests. It's a good practice to
     write tests for your steps to ensure they are working as expected.

6. **Use Transformations**: The `Transform` class in Koheesio allows you to define transformations on your data. It's a
     good practice to encapsulate your transformation logic in `Transform` classes for better readability and
     maintainability.

7. **Consistent Structure**: Koheesio enforces a consistent structure for data processing tasks. Stick to this structure
     to make your codebase easier to understand for new developers.

8. **Use Readers and Writers**: Use the built-in `Reader` and `Writer` classes in Koheesio to handle data extraction
     and loading. This not only simplifies your code but also makes it more robust and efficient.

Remember, these are general best practices and might need to be adapted based on your specific use case and
requirements.


## Pydantic

Koheesio Steps are Pydantic models, which means they can be validated and serialized. This makes it easy to define
the inputs and outputs of a Step, and to validate them before running the Step. Pydantic models also provide a
consistent way to define the schema of the data that a Step expects and produces, making it easier to understand and
maintain the code.

Learn more about Pydantic [here](https://docs.pydantic.dev/latest/).
