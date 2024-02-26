import csv

from matplotlib import pyplot as plt


def main():
    data = {}
    with open("results.csv") as file:
        reader = csv.reader(file)
        for row in reader:
            (
                tree,
                workload,
                iteration,
                preload,
                rw,
                mixed,
                rr,
                update,
                short,
                short_access,
                mid,
                mid_access,
                long,
                long_access,
                empty,
                size,
                depth,
                writes,
                dirty,
                internal,
                leaves,
                redistribute,
                lol_split,
                iqr_move,
                soft_reset,
                hard_reset,
                fast,
            ) = (f.strip() for f in row)
            tokens = workload.split("_")
            workload = f"{tokens[1]}_{tokens[2]}"
            k = int(tokens[1])
            l = int(tokens[2])
            preload = int(preload)
            leaves = int(leaves)
            internal = int(internal)
            redistribute = int(redistribute or 0)
            soft_reset = int(soft_reset or 0)
            hard_reset = int(hard_reset or 0)
            fast = int(fast or 0)
            if tree not in data:
                data[tree] = {
                    "WORKLOAD": [workload],
                    "K": [k],
                    "L": [l],
                    "PRELOAD": [preload],
                    "LEAVES": [leaves],
                    "INTERNAL": [internal],
                    "FAST": [fast],
                    "REDISTRIBUTE": [redistribute],
                    "SOFT_RESET": [soft_reset],
                    "HARD_RESET": [hard_reset],
                }
            else:
                data[tree]["WORKLOAD"].append(workload)
                data[tree]["K"].append(k)
                data[tree]["L"].append(l)
                data[tree]["PRELOAD"].append(preload)
                data[tree]["LEAVES"].append(leaves)
                data[tree]["INTERNAL"].append(internal)
                data[tree]["FAST"].append(fast)
                data[tree]["REDISTRIBUTE"].append(redistribute)
                data[tree]["SOFT_RESET"].append(soft_reset)
                data[tree]["HARD_RESET"].append(hard_reset)

    for name in (
        "PRELOAD",
        "LEAVES",
        "INTERNAL",
        "FAST",
        "REDISTRIBUTE",
        "SOFT_RESET",
        "HARD_RESET",
    ):
        plt.figure(figsize=(12, 6), dpi=200)
        plt.title(name)
        for tree, group in data.items():
            plt.plot(group["WORKLOAD"], group[name], label=tree)
        plt.legend()
        plt.xticks(rotation=45)
        plt.savefig(f"{name}.png")
        plt.clf()


if __name__ == "__main__":
    main()
