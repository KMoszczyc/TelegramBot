import logging
import multiprocessing
import os
import shutil
import time

import pandas as pd

from src.config.paths import MAP_QUIZ_IMAGES_DIR_PATH
from src.models.map_quiz import MapQuiz

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

PARQUET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "misc", "famous_people_trivia.parquet")


def pregenerate_single_image(idx: int) -> dict:
    """Worker function to generate a map image for a single person."""
    try:
        df = pd.read_parquet(PARQUET_PATH)
        person = df.iloc[idx]

        filename = MapQuiz.get_image_filename_for_person(person)
        final_path = os.path.join(MAP_QUIZ_IMAGES_DIR_PATH, filename)

        if os.path.exists(final_path):
            return {"idx": idx, "status": "skipped", "file": filename}

        quiz = MapQuiz()
        locations = MapQuiz.get_locations_for_person(person)

        temp_path = quiz.generate_image(locations)

        # Move from temp to final destination
        shutil.move(temp_path, final_path)

        return {"idx": idx, "status": "generated", "file": filename}
    except Exception as e:
        return {"idx": idx, "status": "error", "error": str(e)}


class MapQuizPregenerator:
    """Handles the bulk pregeneration of Map Quiz images using multiprocessing."""

    def __init__(self, num_cores: int = None):
        self.num_cores = num_cores or min(multiprocessing.cpu_count(), 8)
        self.total_persons = 0
        self.generated = 0
        self.skipped = 0
        self.errors = 0

    def run(self):
        logger.info("Starting Map Quiz image pregeneration...")
        os.makedirs(MAP_QUIZ_IMAGES_DIR_PATH, exist_ok=True)

        if not os.path.exists(PARQUET_PATH):
            logger.error(f"Parquet file not found at {os.path.abspath(PARQUET_PATH)}")
            return

        df = pd.read_parquet(PARQUET_PATH)
        self.total_persons = len(df)
        indices = list(range(self.total_persons))

        logger.info(f"Loaded {self.total_persons} records. Utilizing {self.num_cores} cores.")

        start_time = time.time()

        with multiprocessing.Pool(processes=self.num_cores) as pool:
            for result in pool.imap_unordered(pregenerate_single_image, indices):
                self._handle_result(result)

        duration = time.time() - start_time
        self._print_summary(duration)

    def _handle_result(self, result: dict):
        if result["status"] == "skipped":
            self.skipped += 1
        elif result["status"] == "generated":
            self.generated += 1
        else:
            self.errors += 1
            logger.error(f"Error generating image for index {result['idx']}: {result.get('error')}")

        completed = self.generated + self.skipped + self.errors
        if completed % 10 == 0 or completed == self.total_persons:
            logger.info(
                f"Progress: {completed}/{self.total_persons} (Generated: {self.generated}, Skipped: {self.skipped}, Errors: {self.errors})"
            )

    def _print_summary(self, duration: float):
        logger.info("--- Pregeneration Complete ---")
        logger.info(f"Time taken: {duration:.2f} seconds")
        logger.info(f"Successfully generated: {self.generated}")
        logger.info(f"Skipped (already existed): {self.skipped}")
        logger.info(f"Errors: {self.errors}")


if __name__ == "__main__":
    pregenerator = MapQuizPregenerator()
    pregenerator.run()
