import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sys

def plot(results):
    names = ['tree', 'threads', 'input', 'insert', 'read']

    group = 'tree'
    xaxis = 'threads'

    df = pd.read_csv(results, names=names)
    df[['s', 'k', 'l']] = df['input'].str.split('_', expand=True)
    df['insert_throughput'] = df['s'].astype(int) * 1000000000 / df['insert'].astype(int)
    df['read_throughput'] = df['s'].astype(int) * 10000000 / df['read'].astype(int)

    for s in df['s'].unique():
        temp = df[df['s'] == s]
        for k in temp['k'].unique():
            data = temp[temp['k'] == k]
            for yaxis in ['insert', 'read']:
                plt.clf()
                plt.figure(dpi=400)
                sns.barplot(data, x=xaxis, y=f'{yaxis}_throughput', hue=group)
                plt.ylabel(f'{yaxis} M/s')
                plt.title(f'K {k}% ({s}M)')
                plt.savefig(f'{yaxis}_{k}_{s}.png')


def main():
    results = 'results-v2.csv' if len(sys.argv) < 2 else sys.argv[1]
    plot(results)


if __name__ == "__main__":
    main()
