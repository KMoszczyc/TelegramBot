import cv2
from great_tables import GT, md
import matplotlib.pyplot as plt
import os
import plotly.graph_objects as go

from definitions import TEMP_DIR, DatetimeFormat, PeriodFilterMode
import src.stats.utils as stats_utils


def create_table_plt(df, title, columns):
    fig, ax = plt.subplots()
    fig.patch.set_visible(False)
    ax.axis('off')
    ax.axis('tight')

    tab = ax.table(cellText=df.values, colLabels=columns, loc='center')
    # plt.title(title)

    fig.tight_layout()
    tab.auto_set_column_width(col=list(range(len(columns))))  # Provide integer list of columns to adjust
    plt.subplots_adjust(top=0.85, bottom=0.1)

    fig.canvas.draw()
    bbox = tab.get_window_extent(fig.canvas.get_renderer())
    bbox = bbox.from_extents(bbox.xmin - 3, bbox.ymin - 3, bbox.xmax + 3, bbox.ymax + 3)
    bbox_inches = bbox.transformed(fig.dpi_scale_trans.inverted())

    path = os.path.abspath(os.path.join(TEMP_DIR, stats_utils.generate_random_filename('jpg')))
    stats_utils.create_dir(TEMP_DIR)
    # fig.savefig(path, bbox_inches='tight')
    fig.savefig(path, bbox_inches=bbox_inches)

    return path


def create_table_plotly(df, command_args, columns):
    is_date_range = command_args.period_mode == PeriodFilterMode.DATE_RANGE
    is_date = command_args.period_mode == PeriodFilterMode.DATE_RANGE

    CELL_HEIGHT = 50
    HEADER_CELL_HEIGHT = CELL_HEIGHT*1.65 if is_date_range else CELL_HEIGHT
    CELL_WIDTH = 270
    WIDTH = CELL_WIDTH * 1.2 + CELL_WIDTH * (len(columns) -1) if is_date_range or is_date else CELL_WIDTH * 0.9 + CELL_WIDTH * (len(columns) -1)
    WIDTHS = [CELL_WIDTH * 1.2] + [CELL_WIDTH] * (len(columns) -1) if is_date_range or is_date else [CELL_WIDTH * 0.9] + [CELL_WIDTH] * (len(columns) -1)

    layout = go.Layout(
        autosize=True,
        margin={'l': 0, 'r': 1, 't': 0, 'b': 0},
        height=CELL_HEIGHT * len(df) + HEADER_CELL_HEIGHT + 1,
        width=WIDTH)

    fig = go.Figure(data=[go.Table(
        header=dict(values=list(columns),
                    fill_color='#2E3A46',
                    font=dict(color='#FFFFFF', family='Roboto', size=26),
                    line_color='#4A525A',
                    align='center',
                    height=HEADER_CELL_HEIGHT),
        cells=dict(values=[df[col] for col in df.columns],
                   fill_color=['#2E3A46'] + ['#1B1F24'] * (len(columns) - 1),
                   font=dict(color='#E0E0E0', family='Roboto', size=26),
                   line_color='#4A525A',
                   align='left',
                   height=CELL_HEIGHT),
        columnwidth=WIDTHS)
    ],
        layout=layout)

    path = os.path.abspath(os.path.join(TEMP_DIR, stats_utils.generate_random_filename('jpg')))
    stats_utils.create_dir(TEMP_DIR)

    fig.write_image(path, engine='kaleido')

    return path


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
    start_x, start_y = 0, 0
    end_x, end_y = img.shape[1], img.shape[0]

    x_white_sum = 255 * img_gray.shape[1]
    y_white_sum = 255 * img_gray.shape[0]

    print(img_gray)

    for x in range(img_gray.shape[1]):
        if sum(img_gray[:, x]) < y_white_sum:
            start_x = x
            break
    for x in range(img_gray.shape[1] - 1, 0, -1):
        if sum(img_gray[:, x]) < y_white_sum:
            end_x = x
            break

    for y in range(img_gray.shape[0]):
        if sum(img_gray[y, :]) < x_white_sum:
            start_y = y
            break

    for y in range(img_gray.shape[0] - 1, 0, -1):
        if sum(img_gray[y, :]) < x_white_sum:
            end_y = y
            break

    print(x_white_sum, y_white_sum, start_x, start_y, end_x, end_y)

    img = img[start_y:end_y, start_x:end_x]
    cv2.imwrite(path, img)
