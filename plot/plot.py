import pandas as pd
import matplotlib.pyplot as plt


def plot(df, line, xaxis, yaxis):
    df = df.groupby([line, xaxis])[yaxis].median().reset_index()

    plt.figure(figsize=(9, 6))
    df[['s', 'k', 'l']] = df[xaxis].str.split('_', expand=True)
    df['k'] = df['k'].astype(int)
    df['l'] = df['l'].astype(int)
    df.sort_values(by=['k', 'l'], inplace=True)

    for tree in df[line].unique():
        #if tree != 1:
        #    continue
        exp = df[df[line] == tree]
        plt.plot(exp[xaxis], exp[yaxis], label=tree)

    # plt.ylim(0, 35e8)  # Set the y-axis limits from 0 to 35
    #plt.ylim(0, 2e9)  # Set the y-axis limits from 0 to 35
    plt.xticks(rotation=90)
    plt.legend()
    plt.tight_layout()


def main():
    names = ['tree', 'threads', 'input', 'offset', 'preload', 'raw_write', 'mixed', 'raw_read', 'updates',
             's_range', 's_access', 'm_range', 'm_access', 'l_range', 'l_access',
             'empty', 'size', 'depth', 'writes', 'dirty', 'internal', 'leaves',
             'redistribute', 'split', 'iqr', 'soft', 'hard', 'fp']
    # bench = 'tail'
    # bench = 'simple'
    bench = 'simple_large'
    # bench = 'quit'
    # bench = 'results'
    df = pd.read_csv(f'../{bench}.csv', names=names)
    plot(df, 'threads', 'input', 'preload')
    plt.savefig(f'{bench}.svg')
    # plt.savefig('all.svg')


if __name__ == "__main__":
    main()
