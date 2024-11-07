from __future__ import annotations
from koheesio.steps.http import HttpGetStep
from koheesio.models import Field, PositiveInt


class AirtableGetStep(HttpGetStep):
    """TODO add description"""
    url: str = Field(
        default="https://api.airtable.com/v0/{app_id}/{table_name}/",
        description="Airtable API URL",
        alias="uri",
    )
    app_id: str = Field(default=..., description="Airtable App ID")
    api_key: str = Field(default=..., description="Airtable API key")
    table_name: str = Field(default="Table 1", description="Airtable table name")
    limit: PositiveInt = Field(default=100, description="Maximum number of records to return", alias="pageSize")
    fields: list[str] = Field(default_factory=list, description="Fields to return", min_items=1)

    def get_options(self) -> dict:
        """Generate the parameters for the Airtable API call"""
        params = {"pageSize": self.limit}
        if len(self.fields) > 1:
            params["fields"] = self.fields
        elif len(self.fields) == 1:
            params["fields[]"] = self.fields
        return {
            "url": self.url.format(app_id=self.app_id, table_name=self.table_name),
            "headers": {"Authorization": "Bearer " + self.api_key},
            "params": params,
        }
