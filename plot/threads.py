import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot(bench):
    names = ['tree', 'threads', 'input', 'offset',
             'preload', 'write', 'mixed', 'read', 'update',
             'short', 'short leaves', 'mid', 'mid leaves', 'long', 'long leaves',
             'empty', 'fast', 'fails', 'shared', 'exclusive', 'splits']

    group = 'k'
    xaxis = 'threads'
    yaxis = 'throughput'
    # yaxis = 'exclusive'

    df = pd.read_csv(f'../{bench}.csv', names=names)
    df['input'] = df['input'].str.strip()
    df[['s', 'k', 'l']] = df['input'].str.split('_', expand=True)
    df['throughput'] = df['s'].astype(int) * 1000000000 / df['preload'].astype(int)

    sizes = df['s'].unique()
    if yaxis == 'preload':
        ylims = {'5': 7e9, '50': 8e10, '450': 7e11, '500': 7e11}
    elif yaxis == 'exclusive':
        ylims = {'5': 500000, '50': 500000, '450': None, '500': None}
    elif yaxis == 'throughput':
        ylims = {'5': 30, '50': 30, '450': 30, '500': 30}
    else:
        ylims = {}
    for s in sizes:
        data = df[df['s'] == s]
        plt.clf()
        plt.figure(dpi=400)
        sns.lineplot(data, x=xaxis, y=yaxis, hue=group, marker='X')
        plt.ylim(0, ylims.get(s))
        if yaxis == 'preload':
            plt.ylabel('M inserts/s')
        plt.title(f'{bench} {s}M')
        plt.savefig(f'{yaxis}_{bench}_{s}.png')


def main():
    for bench in ['simple', 'tail', 'quit']:
        plot(bench)


if __name__ == "__main__":
    main()
