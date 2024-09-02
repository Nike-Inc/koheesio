# Context in Koheesio

In the Koheesio framework, the `Context` class plays a pivotal role. It serves as a flexible and powerful tool for 
managing configuration data and shared variables across tasks and steps in your application.

`Context` behaves much like a Python dictionary, but with additional features that enhance its usability and 
flexibility. It allows you to store and retrieve values, including complex Python objects, with ease. You can access 
these values using dictionary-like methods or as class attributes, providing a simple and intuitive interface.

Moreover, `Context` supports nested keys and recursive merging of contexts, making it a versatile tool for managing 
complex configurations. It also provides serialization and deserialization capabilities, allowing you to easily save 
and load configurations in JSON, YAML, or TOML formats.

Whether you're setting up the environment for a Task or Step, or managing variables shared across multiple tasks, 
`Context` provides a robust and efficient solution.

This document will guide you through its key features and show you how to leverage its capabilities in your Koheesio 
applications.

## API Reference

See [API Reference](../../api_reference/context.md) for a detailed description of the `Context` class and its methods.

## Key Features

- **Accessing Values**: `Context` simplifies accessing configuration values. You can access them using dictionary-like 
  methods or as class attributes. This allows for a more intuitive interaction with the `Context` object. For example:

    ```python
    context = Context({"bronze_table": "catalog.schema.table_name"})
    print(context.bronze_table)  # Outputs: catalog.schema.table_name
    ```

- **Nested Keys**: `Context` supports nested keys, allowing you to access and add nested keys in a straightforward way. 
  This is useful when dealing with complex configurations that require a hierarchical structure. For example:

    ```python
    context = Context({"bronze": {"table": "catalog.schema.table_name"}})
    print(context.bronze.table)  # Outputs: catalog.schema.table_name
    ```

- **Merging Contexts**: You can merge two `Contexts` together, with the incoming `Context` having priority. Recursive 
  merging is also supported. This is particularly useful when you want to update a `Context` with new data without 
  losing the existing values. For example:

    ```python
    context1 = Context({"bronze_table": "catalog.schema.table_name"})
    context2 = Context({"silver_table": "catalog.schema.table_name"})
    context1.merge(context2)
    print(context1.silver_table)  # Outputs: catalog.schema.table_name
    ```

- **Adding Keys**: You can add keys to a Context by using the `add` method. This allows you to dynamically update the 
  `Context` as needed. For example:

    ```python
    context.add("silver_table", "catalog.schema.table_name")
    ```

- **Checking Key Existence**: You can check if a key exists in a Context by using the `contains` method. This is useful 
  when you want to ensure a key is present before attempting to access its value. For example:

    ```python
    context.contains("silver_table")  # Returns: True
    ```

- **Getting Key-Value Pair**: You can get a key-value pair from a Context by using the `get_item` method. This can be 
  useful when you want to extract a specific piece of data from the `Context`. For example:

    ```python
    context.get_item("silver_table")  # Returns: {"silver_table": "catalog.schema.table_name"}
    ```

- **Converting to Dictionary**: You can convert a Context to a dictionary by using the `to_dict` method. This can be 
  useful when you need to interact with code that expects a standard Python dictionary. For example:

    ```python
    context_dict = context.to_dict()
    ```

- **Creating from Dictionary**: You can create a Context from a dictionary by using the `from_dict` method. This allows 
  you to easily convert existing data structures into a `Context`. For example:

    ```python
    context = Context.from_dict({"bronze_table": "catalog.schema.table_name"})
    ```

## Advantages over a Dictionary

While a dictionary can be used to store configuration values, `Context` provides several advantages:

- **Support for nested keys**: Unlike a standard Python dictionary, `Context` allows you to access nested keys as if 
  they were attributes. This makes it easier to work with complex, hierarchical data.

- **Recursive merging of two `Contexts`**: `Context` allows you to merge two `Contexts` together, with the incoming 
  `Context` having priority. This is useful when you want to update a `Context` with new data without losing the 
  existing values.

- **Accessing keys as if they were class attributes**: This provides a more intuitive way to interact with the 
  `Context`, as you can use dot notation to access values.

- **Code completion in IDEs**: Because you can access keys as if they were attributes, IDEs can provide code completion 
  for `Context` keys. This can make your coding process more efficient and less error-prone.

- **Easy creation from a YAML, JSON, or TOML file**: `Context` provides methods to easily load data from YAML or JSON 
  files, making it a great tool for managing configuration data.

## Data Formats and Serialization

`Context` leverages JSON, YAML, and TOML for serialization and deserialization. These formats are widely used in the 
industry and provide a balance between readability and ease of use.

- **JSON**: A lightweight data-interchange format that is easy for humans to read and write and easy for machines to 
  parse and generate. It's widely used for APIs and web-based applications.

- **YAML**: A human-friendly data serialization standard often used for configuration files. It's more readable than 
  JSON and supports complex data structures.

- **TOML**: A minimal configuration file format that's easy to read due to its clear and simple syntax. It's often used 
  for configuration files in Python applications.

## Examples

In this section, we provide a variety of examples to demonstrate the capabilities of the `Context` class in Koheesio.

### Basic Operations

Here are some basic operations you can perform with `Context`. These operations form the foundation of how you interact 
with a `Context` object:

```python
# Create a Context
context = Context({"bronze_table": "catalog.schema.table_name"})

# Access a value
value = context.bronze_table

# Add a key
context.add("silver_table", "catalog.schema.table_name")

# Merge two Contexts
context.merge(Context({"silver_table": "catalog.schema.table_name"}))
```

### Serialization and Deserialization

`Context` supports serialization and deserialization to and from JSON, YAML, and TOML formats. This allows you to 
easily save and load `Context` data:

```python
# Load context from a JSON file
context = Context.from_json("path/to/context.json")

# Save context to a JSON file
context.to_json("path/to/context.json")

# Load context from a YAML file
context = Context.from_yaml("path/to/context.yaml")

# Save context to a YAML file
context.to_yaml("path/to/context.yaml")

# Load context from a TOML file
context = Context.from_toml("path/to/context.toml")

# Save context to a TOML file
context.to_toml("path/to/context.toml")
```

### Nested Keys

`Context` supports nested keys, allowing you to create hierarchical configurations. This is useful when dealing with 
complex data structures:

```python
# Create a Context with nested keys
context = Context({
    "database": {
        "bronze_table": "catalog.schema.bronze_table",
        "silver_table": "catalog.schema.silver_table",
        "gold_table": "catalog.schema.gold_table"
    }
})

# Access a nested key
print(context.database.bronze_table)  # Outputs: catalog.schema.bronze_table
```

### Recursive Merging

`Context` also supports recursive merging, allowing you to merge two `Contexts` together at all levels of their 
hierarchy. This is particularly useful when you want to update a `Context` with new data without losing the existing 
values:

```python
# Create two Contexts with nested keys
context1 = Context({
    "database": {
        "bronze_table": "catalog.schema.bronze_table",
        "silver_table": "catalog.schema.silver_table"
    }
})

context2 = Context({
    "database": {
        "silver_table": "catalog.schema.new_silver_table",
        "gold_table": "catalog.schema.gold_table"
    }
})

# Merge the two Contexts
context1.merge(context2)

# Print the merged Context
print(context1.to_dict())  
# Outputs: 
# {
#     "database": {
#         "bronze_table": "catalog.schema.bronze_table",
#         "silver_table": "catalog.schema.new_silver_table",
#         "gold_table": "catalog.schema.gold_table"
#     }
# }
```

## Jsonpickle and Complex Python Objects

The `Context` class in Koheesio also uses `jsonpickle` for serialization and deserialization of complex Python objects 
to and from JSON. This allows you to convert complex Python objects, including custom classes, into a format that can 
be easily stored and transferred.

Here's an example of how this works:

```python
# Import necessary modules
from koheesio.context import Context

# Initialize SnowflakeReader and store in a Context
snowflake_reader = SnowflakeReader(...)  # fill in with necessary arguments
context = Context({"snowflake_reader": snowflake_reader})

# Serialize the Context to a JSON string
json_str = context.to_json()

# Print the serialized Context
print(json_str)

# Deserialize the JSON string back into a Context
deserialized_context = Context.from_json(json_str)

# Access the deserialized SnowflakeReader
deserialized_snowflake_reader = deserialized_context.snowflake_reader

# Now you can use the deserialized SnowflakeReader as you would the original
```

This feature is particularly useful when you need to save the state of your application, transfer it over a network, 
or store it in a database. When you're ready to use the stored data, you can easily convert it back into the original 
Python objects.

However, there are a few things to keep in mind:

1. The classes you're serializing must be importable (i.e., they must be in the Python path) when you're deserializing 
   the JSON. `jsonpickle` needs to be able to import the class to reconstruct the object. This holds true for most
   Koheesio classes, as they are designed to be importable and reconstructible.

2. Not all Python objects can be serialized. For example, objects that hold a reference to a file or a network 
   connection can't be serialized because their state can't be easily captured in a static file.

3. As mentioned in the code comments, `jsonpickle` is not secure against malicious data. You should only deserialize 
   data that you trust.

So, while the `Context` class provides a powerful tool for handling complex Python objects, it's important to be aware 
of these limitations.

## Conclusion

In this document, we've covered the key features of the `Context` class in the Koheesio framework, including its 
ability to handle complex Python objects, support for nested keys and recursive merging, and its serialization and 
deserialization capabilities. 

Whether you're setting up the environment for a Task or Step, or managing variables shared across multiple tasks, 
`Context` provides a robust and efficient solution.

## Further Reading

For more information, you can refer to the following resources:

- [Python jsonpickle Documentation](https://jsonpickle.github.io/)
- [Python JSON Documentation](https://docs.python.org/3/library/json.html)
- [Python YAML Documentation](https://pyyaml.org/wiki/PyYAMLDocumentation)
- [Python TOML Documentation](https://toml.readthedocs.io/en/latest/)

Refer to the API documentation for more details on the `Context` class and its methods.
