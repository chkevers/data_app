from typing import Dict, List, Union
from typing_extensions import Literal, TypeAlias
from airflow.models import Variable, BaseOperator

# LayerSpec: TypeAlias = Dict[Union[Literal["fl"],Literal["pl"],Literal["al"]], List[BaseOperator]]
test = Dict[Union[Literal["fl"],Literal["pl"],Literal["al"]], List[BaseOperator]]
print(test)