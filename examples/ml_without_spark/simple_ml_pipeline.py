"""
Example: ML Feature Engineering Pipeline without Spark

This example demonstrates how to use Koheesio for ML workflows WITHOUT requiring PySpark.
Install with: pip install koheesio[ml]

This showcases the core value proposition: lightweight ML pipelines using pandas transformations.
"""

import pandas as pd
from koheesio.pandas.transformations import Transformation


# Sample dataset
data = pd.DataFrame({
    'age': [25, 30, 35, 40, 45, 50, 55, 60],
    'income': [30000, 45000, 55000, 65000, 75000, 85000, 95000, 105000],
    'credit_score': [650, 680, 700, 720, 740, 760, 780, 800],
    'loan_amount': [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000]
})


class AgeGroupTransformation(Transformation):
    """Categorize age into groups"""
    
    target_column: str = "age_group"
    
    def execute(self):
        self.output.df = self.df.copy()
        self.output.df[self.target_column] = pd.cut(
            self.df['age'],
            bins=[0, 30, 45, 100],
            labels=['young', 'middle', 'senior']
        )


class IncomeRatioTransformation(Transformation):
    """Calculate loan-to-income ratio"""
    
    target_column: str = "loan_to_income_ratio"
    
    def execute(self):
        self.output.df = self.df.copy()
        self.output.df[self.target_column] = (
            self.df['loan_amount'] / self.df['income']
        ).round(2)


class CreditScoreNormalization(Transformation):
    """Normalize credit score to 0-1 range"""
    
    target_column: str = "credit_score_normalized"
    
    def execute(self):
        self.output.df = self.df.copy()
        min_score = self.df['credit_score'].min()
        max_score = self.df['credit_score'].max()
        self.output.df[self.target_column] = (
            (self.df['credit_score'] - min_score) / (max_score - min_score)
        ).round(3)


class RiskScoreTransformation(Transformation):
    """Calculate a simple risk score based on multiple factors"""
    
    target_column: str = "risk_score"
    
    def execute(self):
        self.output.df = self.df.copy()
        # Simple risk score: higher loan-to-income ratio and lower credit score = higher risk
        self.output.df[self.target_column] = (
            self.df['loan_to_income_ratio'] * 0.6 + 
            (1 - self.df['credit_score_normalized']) * 0.4
        ).round(3)


def main():
    """Run the ML feature engineering pipeline"""
    
    print("=" * 80)
    print("Koheesio ML Pipeline Example - No Spark Required!")
    print("=" * 80)
    print()
    
    print("Original Data:")
    print(data)
    print()
    
    # Build the pipeline using pandas transformations
    # Note: No Spark required!
    result = (
        data
        .pipe(AgeGroupTransformation())
        .pipe(IncomeRatioTransformation())
        .pipe(CreditScoreNormalization())
        .pipe(RiskScoreTransformation())
    )
    
    print("After Feature Engineering:")
    print(result)
    print()
    
    print("Summary Statistics:")
    print(result[['loan_to_income_ratio', 'credit_score_normalized', 'risk_score']].describe())
    print()
    
    print("Pipeline completed successfully!")
    print("All transformations ran without Spark")
    print()
    print("Key Benefits:")
    print("  - Lightweight: No Spark installation required")
    print("  - Fast: Runs on local machine with pandas")
    print("  - Flexible: Can switch to Spark transformations when needed")
    print("  - Type-safe: Full Pydantic validation")
    

if __name__ == "__main__":
    main()
