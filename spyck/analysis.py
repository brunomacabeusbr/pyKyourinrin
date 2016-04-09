import matplotlib.pyplot as plt
import matplotlib.colors


def make_pie(dates, split=None):
    plt.rcParams['font.size'] = 8

    dates_to_pie = []

    if split is not None:
        split_part1 = {k: v for k, v in dates.items() if eval(str(v) + split)}
        dates_to_pie.append(sorted(split_part1.items(), key=lambda x: x[1]))

        split_part2 = {k: v for k, v in dates.items() if not eval(str(v) + split)}
        dates_to_pie.append(sorted(split_part2.items(), key=lambda x: x[1]))
    else:
        dates_to_pie.append(sorted(dates.items(), key=lambda x: x[1]))

    for current_dates in dates_to_pie:
        labels = [i[0] for i in current_dates]
        sizes = [i[1] for i in current_dates]
        plt.pie(sizes, startangle=90, labels=labels, labeldistance=1, colors=list(matplotlib.colors.cnames.keys()), autopct='%1.1f%%')
        plt.axis('equal')
        plt.tight_layout()
        plt.show()


def print_percent(a, b, title=None, name_a=None, name_b=None):
    if title is not None:
        print(title)

    if name_a is not None:
        print(name_a, end=': ')

    print(a)

    if name_b is not None:
        print(name_b, end=': ')

    print(b)

    print('Percent: ' + str(b * 100 / a))
