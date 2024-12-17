import pandas.testing as pdt

from koheesio.pandas import pandas as pd
from koheesio.pandas.transformations import Transformation

# create a DataFrame with 3 rows
input_df = pd.DataFrame({"old_column": [0, 1, 2]})


class AddOne(Transformation):
    """A transformation that adds 1 to a column and stores the result in a given target_column."""

    target_column: str = "new_column"

    def execute(self):
        self.output.df = self.df.copy()
        self.output.df[self.target_column] = self.df["old_column"] + 1


class TestTransformation:
    """Test that the AddOne transformation adds 1 to the old_column and stores the result in the new_column.

    AddOne is used simply as an example of a transformation for testing purposes.
    This same example class is used in the docstring of the AddOne transformation.
    """

    def test_add_one_execute(self):
        """When using execute, the input df should be explicitly set in the constructor."""
        # Arrange
        add_one = AddOne(df=input_df)
        expected_df = pd.DataFrame({"old_column": [0, 1, 2], "new_column": [1, 2, 3]})
        # Act
        add_one.execute()
        # Assert
        pdt.assert_frame_equal(add_one.output.df, expected_df)

    def test_add_one_transform(self):
        # Arrange
        add_one = AddOne(target_column="foo")
        expected_df = pd.DataFrame({"old_column": [0, 1, 2], "foo": [1, 2, 3]})
        # Act
        output_df = add_one.transform(input_df)
        # Assert
        pdt.assert_frame_equal(output_df, expected_df)

    def test_add_one_pipe(self):
        """Test that the AddOne transformation can be used as a function in a DataFrame's pipe method."""
        # Arrange
        add_one = AddOne(target_column="foo")
        add_baz = AddOne(target_column="baz")
        expected_df = pd.DataFrame({"old_column": [0, 1, 2], "foo": [1, 2, 3], "bar": [1, 2, 3], "baz": [1, 2, 3]})
        # Act
        output_df = (
            input_df
            # using the transform method
            .pipe(add_one.transform)
            # using the transformation as a function
            .pipe(AddOne(target_column="bar"))
            # or, using a variable that holds the transformation
            .pipe(add_baz)
        )
        # Assert
        pdt.assert_frame_equal(output_df, expected_df)
