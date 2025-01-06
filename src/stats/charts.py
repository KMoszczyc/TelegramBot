import cv2
from great_tables import GT, md
import matplotlib.pyplot as plt
import os

from definitions import TEMP_DIR
import src.stats.utils as stats_utils

def create_table(df, title, columns, source_notes):
    df.columns = columns
    gt = GT(df).tab_header(title=title).fmt_markdown(columns=columns)

    if source_notes is not None:
        for note in source_notes:
            gt = gt.tab_source_note(source_note=md(note))
    # gt.show()

    path = os.path.abspath(os.path.join(TEMP_DIR, stats_utils.generate_random_filename('png')))
    stats_utils.create_dir(TEMP_DIR)
    gt.save(path)
    print('path', path)
    cut_excess_white_space_from_image(path)

    return path


def cut_excess_white_space_from_image(path):
    img = cv2.imread(path)
    img_gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    start_x = 0
    end_x = img.shape[1]
    end_y = img.shape[0]

    x_white_sum = 255 * img_gray.shape[1]
    y_white_sum = 255 * img_gray.shape[0]


    for x in range(img_gray.shape[1]):
        if sum(img_gray[:, x]) < y_white_sum:
            start_x = x
            break
    for x in range(img_gray.shape[1]-1, 0, -1):
        if sum(img_gray[:, x]) < y_white_sum:
            end_x = x
            break

    for y in range(img_gray.shape[0]-1, 0, -1):
        if sum(img_gray[y, :]) < x_white_sum:
            end_y = y
            break

    img = img[0:end_y, start_x:end_x]
    cv2.imwrite(path, img)
