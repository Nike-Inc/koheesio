## Scope

Koheesio adopts a comprehensive documentation approach that combines inline code documentation with structured external documentation. This document outlines our documentation standards and practices.

### Code Documentation Standards

#### Docstring Format

All Koheesio classes, functions, and modules use **NumPy-style docstrings** with the following structure:

1. **Brief Description**: A concise one-line summary at the top
2. **Extended Description**: Optional detailed explanation (separated by a blank line)
3. **Parameters**: Description of function/method parameters with types
4. **Returns**: Description of return values (if applicable)
5. **Notes**: Additional information, caveats, or important details
6. **Examples**: Code examples demonstrating usage
7. **See Also**: References to related components

#### Markdown in Docstrings

Koheesio docstrings support **markdown formatting** for better readability:

- Headers using `###` or underlined with `---`
- Code blocks with triple backticks
- Bullet points with `*` or `-`
- Inline code with single backticks
- Examples are to use code blocks `\`\`\`python ... \`\`\``

#### Example: Step Documentation

```python
class MyReader(Reader):
    """Reads data from an external source.

    This reader connects to an external data source and loads data into a Spark DataFrame.
    It supports both batch and streaming modes.

    Parameters
    ----------
    source_path : str
        Path to the data source
    format : str
        Data format (e.g., 'json', 'csv', 'parquet')
    options : Optional[Dict[str, Any]]
        Additional options for the reader

    Notes
    -----
    * Make sure the necessary dependencies are available in your Spark session
    * For streaming mode, set `streaming=True`

    Examples
    --------
    ```python
    from koheesio.spark.readers import MyReader

    reader = MyReader(
        source_path="s3://bucket/data",
        format="parquet"
    )
    df = reader.execute().df
    ```

    See Also
    --------
    * [Reader](../reference/reader.md): Base reader class
    * [Writer](../reference/writer.md): For writing data
    """

    source_path: str = Field(default=..., description="Path to the data source")
    format: str = Field(default="parquet", description="Data format to read")
```

#### Field Documentation
All Pydantic fields in Koheesio classes must include descriptions using the `description` parameter:

```python
class MyStep(Step):
    input_column: str = Field(
        default=...,
        description="Name of the input column to process"
    )
    threshold: int = Field(
        default=100,
        description="Threshold value for filtering"
    )
```

### Documentation Structure

Koheesio follows **The Documentation System** (Divio) with four documentation types:

1. **Tutorials**: Learning-oriented guides for newcomers
2. **How-To Guides**: Task-oriented guides for specific problems
3. **Reference**: Information-oriented technical documentation
4. **Explanation**: Understanding-oriented conceptual documentation

All external documentation is maintained in the `docs/` directory and built using MkDocs.

## The System

We will be adopting "The Documentation System".

From [documentation.divio.com](https://documentation.divio.com):

> There is a secret that needs to be understood in order to write good software _documentation_: there isn’t one thing called documentation, there are four.
>
> They are: _tutorials, how-to guides, technical reference and explanation_. They represent four different purposes or functions, and require four different approaches to their creation. Understanding the implications of this will help improve most documentation - often immensely.
>
> **About the system**
> ![Documentation System Overview](../assets/documentation-system-overview.png)
> The documentation system outlined here is a simple, comprehensive and nearly universally-applicable scheme. It is proven in practice across a wide variety of fields and applications.
>
> There are some very simple principles that govern documentation that are very rarely if ever spelled out. They seem to be a secret, though they shouldn’t be.
>
> If you can put these principles into practice, **it will make your documentation better and your project, product or team more successful** - that’s a promise.
>
> [The system is widely adopted](https://documentation.divio.com/adoption.html#adoption) for large and small, open and proprietary documentation projects.
>
> Video Presentation on YouTube:
>
> [![Documentation System Video Presentation](http://img.youtube.com/vi/t4vKPhjcMZg/0.jpg)](http://www.youtube.com/watch?v=t4vKPhjcMZg "What nobody tells you about documentation")
