from pydantic import BaseModel, ConfigDict


class ExtraAllowModel(BaseModel):
    model_config = ConfigDict(extra="allow")
