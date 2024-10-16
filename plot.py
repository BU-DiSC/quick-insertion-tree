import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sys
    
def plot(results):
    names = ['tree', 'input', 'insert', 'read']

    group = 'tree'
    xaxis = 'k'

    df = pd.read_csv(results, names=names)
    df[['s', 'k', 'l']] = df['input'].str.split('_', expand=True)
    df['insert_throughput'] = df['s'].astype(int) * 1000000000 / df['insert'].astype(int)
    df['read_throughput'] = df['s'].astype(int) * 10000000 / df['read'].astype(int)

    for s in df['s'].unique():
        data = df[df['s'] == s]
        for yaxis in ['insert', 'read']:
            plt.clf()
            plt.figure(dpi=400)
            sns.barplot(data, x=xaxis, y=f'{yaxis}_throughput', hue=group)
            plt.ylabel(f'{yaxis} M/s')
            plt.savefig(f'{yaxis}_{s}.png')


def main():
    results = 'results.csv' if len(sys.argv) == 1 else sys.argv[1]
    plot(results)


if __name__ == "__main__":
    main()
