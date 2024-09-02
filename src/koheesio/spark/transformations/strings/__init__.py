"""
Adds a number of Transformations that are intended to be used with StringType column input.
Some will work with other types however, but will output StringType or an array of StringType.

These Transformations take full advantage of Koheesio's ColumnsTransformationWithTarget class, allowing a user to
apply column transformations to multiple columns at once. See the class docstrings for more information.

The following Transformations are included:

[change_case](change_case.md):

- `Lower`
    Converts a string column to lower case.
- `Upper`
    Converts a string column to upper case.
- `TitleCase` or `InitCap`
    Converts a string column to title case, where each word starts with a capital letter.

[concat](concat.md):

- `Concat`
    Concatenates multiple input columns together into a single column, optionally using the given separator.

[pad](pad.md):

- `Pad`
    Pads the values of `source_column` with the `character` up until it reaches `length` of characters
- `LPad`
    Pad with a character on the left side of the string.
- `RPad`
    Pad with a character on the right side of the string.

[regexp](regexp.md):

- `RegexpExtract`
    Extract a specific group matched by a Java regexp from the specified string column.
- `RegexpReplace`
    Searches for the given regexp and replaces all instances with what is in 'replacement'.

[replace](replace.md):

- `Replace`
    Replace all instances of a string in a column with another string.

[split](split.md):

- `SplitAll`
    Splits the contents of a column on basis of a split_pattern.
- `SplitAtFirstMatch`
    Like SplitAll, but only splits the string once. You can specify whether you want the first or second part.

[substring](substring.md):

- `Substring`
    Extracts a substring from a string column starting at the given position.

[trim](trim.md):

- `Trim`
    Trim whitespace from the beginning and/or end of a string.
- `LTrim`
    Trim whitespace from the beginning of a string.
- `RTrim`
    Trim whitespace from the end of a string.

"""
