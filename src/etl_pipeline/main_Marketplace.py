from src.etl_pipeline.extract.extract import Extract
from src.etl_pipeline.transform.transform_Marketplace import Transform
from src.etl_pipeline.load.load_code import Load


def run():
    raw_data = Extract("raw_data.csv", "segments.csv")
    processed_data = Transform(raw_data.extract())
    Load(processed_data)


if __name__ == "__main__":
    run()
