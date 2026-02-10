from src.etl_pipeline.extract.extract_code import Extract
from src.etl_pipeline.transform.transform_code import Transform
from src.etl_pipeline.load.load_code import Load

def run():
    raw_data = Extract()
    processed_data = Transform(raw_data)
    Load(processed_data)

if __name__ == "__main__":
    run()
