from pandas import DataFrame 
from json import loads

from keyuri.config.BaseConfig import BaseConfig

file_name = "w106_sample_feature.csv"

json_arr = []
with open(file_name, "r") as handle:
    line = handle.readline().rstrip().replace('\'', '"')
    while line:
        json_obj = loads(line)
        print(json_obj)
        json_arr.append(json_obj)
        line = handle.readline().rstrip().replace('\'', '"')

config = BaseConfig()
sample_feature_path = config.get_sample_feature_file_path("basic", "w106")

sample_feature_path.parent.mkdir(exist_ok=True, parents=True)

print(json_arr)
df = DataFrame(json_arr)
print(df)
df.to_csv(sample_feature_path, index=False)
