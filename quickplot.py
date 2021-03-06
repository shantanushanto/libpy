import seaborn as sns
import matplotlib.pyplot as plt
import os
import numpy as np
from copy import deepcopy


def heatmap(mat, normalize=2, title=''):
    # Given a 2d matrix produce a heatmap
    # normalization 0: none 1: l1 norm 2: max
    X = np.array(mat)
    if normalize == 1:
        linfnorm = np.linalg.norm(X, axis=1, ord=np.inf)
        X = X.astype(np.float) / linfnorm[:, None]
    elif normalize == 2:
        mx = np.max(X)
        if mx is not 0:
            X /= mx
    sns.heatmap(X, cmap="Blues")
    plt.title(f'{title}, norm: {normalize}')
    plt.show()


def bar(vec, normalize=2, title=''):
    # Given a 1d vector produce a distribution graph
    # normalization 0: none 1: l1 norm 2: max
    X = np.array(vec)
    if normalize == 1:
        norm = np.linalg.norm(X)
        norm_arr = X / norm
        X = norm_arr
    elif normalize == 2:
        mx = np.max(X)
        if mx is not 0:
            X /= mx
    x = list(range(len(X)))
    y = X
    sns.barplot(x=x, y=y, color='blue')
    plt.xticks(rotation=90)
    plt.title(f'{title}, norm: {normalize}')
    plt.grid()
    plt.show()


def line(vec, title='', save_path=None):
    # given a vector draw line plot
    plt.plot(vec)
    plt.title(title)
    plt.grid()

    if save_path:
        fig_name = title.replace(' ', '_')
        path = os.path.join(save_path, fig_name)
        plt.savefig(path)

    plt.show()
def scatter(vec, title=''):
    # vec is a n*2 list. First and second column is x and y axis correspondingly.
    # separating data from vec
    x, y = [], []
    for d in vec:
        x.append(d[0])
        y.append(d[1])
    plt.scatter(x=x, y=y)
    plt.title(title)
    plt.grid()
    plt.show()
