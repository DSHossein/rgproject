import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os


def read_data(path):
    df = pd.read_csv(path, index_col=0)
    return df


def save_png(df, output_dir='.'):
    os.makedirs(output_dir, exist_ok=True)
    datestring = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    fig_file = os.path.join(output_dir, 'Android_' + datestring + '.png')

    df.plot(figsize=(20, 10), linewidth=4)
    plt.grid()
    plt.savefig(fig_file, bbox_inches='tight')


def csv_to_png(csv_path, output_dir='.'):
    dfm = read_data(csv_path)
    save_png(dfm, output_dir)









