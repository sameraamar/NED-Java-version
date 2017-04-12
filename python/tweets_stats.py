from pprint import pprint
import pandas
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.mlab as mlab
try:
    import powerlaw
except:
    print("try to install using:\npip install powerlaw")
    exit()

import math, numpy as np


# find the first bin for the given binsize
def bin_find_k(binsize):
    print
    'calculate bin for binsize = ', binsize
    k = 1
    flag = True
    while flag:
        logk = math.log10(k)
        logk1 = math.log10(k + 1)

        print
        k, '\t', logk, '\t', logk1, '\t', abs(logk1 - logk)
        if abs(logk1 - logk) <= binsize:
            flag = False

        k += 1

    return k


def findBin(val, bins):
    for j in range(len(bins) + 1):
        # print i, j, logi >= bins[j] , logi < bins[j+1]
        if val >= bins[j] and val < bins[j + 1]:
            return j
    return -1

def log_hist(arr, maxBinSize):
    K = bin_find_k(maxBinSize)

    #logY = np.log10(arr)
    size = len(arr)
    logX = np.log10(range(size))

    # calculate bin array
    print
    "Min and max:"
    minV = logX[K]  # np.min(logX[k:])
    maxV = logX[len(logX) - 1]  # np.max(logX[k:])

    print
    minV, maxV
    # create log bins: (by specifying the multiplier)
    bins = [minV]
    cur_value = bins[0]
    while cur_value <= maxV:
        cur_value = cur_value + maxBinSize
        bins.append(cur_value)

    bin_indices = {}
    for i in range(K, len(arr)):
        j = findBin(np.log10(i), bins)
        if j not in bin_indices:
            bin_indices[j] = []
        bin_indices[j].append(i)

    # %%

    newarr = arr[:]  # copy values to a new array
    average = [0] * len(bins)
    currbin = K
    for b in bin_indices:
        for ind in bin_indices[b]:
            average[b] += newarr.iloc[ind]
        average[b] = average[b] / len(bin_indices[b])  # average

        for ind in bin_indices[b]:
            newarr[ind] = average[b]

    return bins, average

def np_hist(actions, title, ax, color, log=False, bins=100, normalize=False):
    hist, edges = np.histogram(actions, bins=bins, density=normalize)
    if normalize:
        hist = hist / len(actions)
    #if(normed):
    #    weights = np.ones_like(feature) / len(feature)
    #    ax.hist(feature, bins=100, weights=weights, histtype='bar', color="red")

    colors = [color]

    ax.bar(edges[:-1], hist, width=1, color=color, label=title)
    ax.legend(prop={'size': 10})
    ax.set_title('bars with legend')


def hist(feature, ax, title, xlabel, ylabel, bins=100, normed=0, log=False, color="blue", ignoreZeros=True):
    #np_hist(feature, title, ax, color, log=log, bins=bins, normalize=(normed == 1))

    #feature = feature[feature > 0]

    log = False
    if log:
        bins, average = log_hist(feature, 0.01)
        ax.plot(bins, average)
        #plt.show()
    else:
        hist, xbins = np.histogram(feature, bins=bins)
        #xbins = np.linspace(0, )

        tmp1 = [np.sum(xbins[0:9])]
        center = (xbins[:-1] + xbins[1:]) / 2
        width = np.diff(xbins)

        ax.bar(center, hist, align='center', width=width, color=color)
        #ax.set_xticks(xbins)

        #ax.hist(bins, histtype='bar', normed=normed, log=log, color=color)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)

def hist1(feature, ax, title, xlabel, ylabel, bins=100, normed=0, log=False, color="blue"):
    # np_hist(feature, title, ax, color, log=log, bins=bins, normalize=(normed == 1))

    #feature = feature[feature > 0]

    #if log:
    #    bins, average = log_hist(feature, 0.1)
    #    ax.plot(bins, average)
    #    # plt.show()
    #else:
    ax.hist(feature, bins=bins, histtype='bar', normed=normed, log=log, color=color)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)


#sample histogram: http://matplotlib.org/examples/statistics/histogram_demo_multihist.html

def retweets(events, no_events):

    feature_no_events = no_events['retweets']
    feature_events = events['retweets']

    feature_hist(feature_events, feature_no_events, 'retweets')

    analyze(feature_no_events, feature_name='retweets-no events')
    analyze(feature_events, feature_name='retweets-events')

def analyze(data, feature_name):
    print("Analyzing feature:", feature_name)
    results = powerlaw.Fit(data)
    print ("alpha=", results.power_law.alpha)
    print("segma=", results.power_law.sigma)
    print("xmin=", results.power_law.xmin)
    R, p = results.distribution_compare('power_law', 'lognormal')

    print("R=", R, "p=", p)
    #fig2 = plt.figure()
    #ax = fig2.add_axes()
    #results.plot_cdf(ax=ax) #color = 'b', linewidth = 2)
    #plt.show()

def likes(events, no_events):
    feature_no_events = no_events['likes']
    feature_events = events['likes']

    feature_hist(feature_events, feature_no_events, 'likes')

    analyze(feature_no_events, feature_name='likes-no events')
    analyze(feature_events, feature_name='retwelikesets-events')

def calcGroups(events, no_events):
    group_no_event = no_events.groupby('group')
    group_event    = events.groupby('group')

    agg_no_events = group_no_event.agg(['count'])
    agg_events    = group_event.agg(['count'])

    feature_events = agg_events['id']['count']
    feature_no_events = agg_no_events['id']['count']
    feature_hist(feature_events, feature_no_events, 'groups')

    analyze(feature_no_events, feature_name='group-no events')
    analyze(feature_events, feature_name='group-events')

    print("done 'calcGroups'")

    #fig, ax = plt.subplots(figsize=(8, 6))
    #for label, df in groups:
    #    df.vals.plot(kind="kde", ax=ax, label=label)
    #plt.legend()





def feature_hist(feature_events, feature_no_events, feature_name):

    fig, axes = plt.subplots(nrows=3, ncols=2)
    ax0, ax1, ax2, ax3, ax4, ax5 = axes.flatten()

    bins = 50

    hist (feature_no_events, ax0, feature_name+' - no events (hist)', feature_name, 'count', bins=bins, color="red")
    hist1(feature_no_events, ax2, feature_name+' - no events (hist1)', feature_name, 'count', bins=bins, color="red")

    #hist(feature_no_events, ax2, feature_name+' - no events', feature_name, 'count2', bins=bins, color="red")
    #hist(feature_no_events, ax4, feature_name+' (log) - no events', feature_name, 'log(count)', log=True, bins=bins, color="red")

    hist (feature_events, ax1, feature_name+' - events (hist)', feature_name, 'count', bins=bins, color="blue")
    hist1(feature_events, ax3, feature_name+' - events (hist1)', feature_name, 'count', bins=bins, color="blue")
    #hist(feature_events, ax3, feature_name+' - events', feature_name, 'count2', bins=bins, color="blue")
    #hist(feature_events, ax3, feature_name+' (normalized) - events', normed=1, bins=bins, color="blue")
    #hist(feature_events, ax5, feature_name+' (log) - events', feature_name, 'log(count)', bins=bins, log=True, color="blue")

    # weights = np.ones_like(feature_no_events) / len(feature_no_events)
    # ax2.hist(feature_no_events, bins=bins, weights=weights, histtype='bar', color="red")


    fig.tight_layout()
    plt.show()

    print("Plotted features for", feature_name)


def analyzeDataset(filename):
    np.seterr(divide='ignore', invalid='ignore')

    dataset = pandas.read_csv(filename)
    dataset['jRtwt'] = dataset['jRtwt'].astype(str)

    #dataset[dataset['retweets'] == 0] = 0.001

    #print(len(dataset[dataset['retweets'] == 0]))
    #exit()

    #analyze(dataset['likes'], 'likes')
    #exit()

    no_events = dataset[dataset['topic_id'] == -1]
    events = dataset[dataset['topic_id'] > -1]

    print("summary: ")

    groups = dataset.groupby(by='topic_id')
    agg = groups.agg(['count', 'max'])
    print(agg['level'], sep='\t')

    print("found", len(events), "tweets with", len(groups.groups)-1, "events")
    print("found", len(no_events), "tweets without any specific events")

    retweets(events, no_events)
    likes(events, no_events)
    calcGroups(events, no_events)


    #likes0 = likes0[likes0 > 10]
    #likes1 = likes1[likes1 > 10]

    #hist0 = np.histogram(likes0)
    #hist1 = np.histogram(likes1)


    ##print (group)
    #plt.title("Gaussian Histogram")
    #plt.xlabel("Value")
    #plt.ylabel("Frequency")

if __name__ == "__main__":
    analyzeDataset('c:/temp/dataset-winehouse.txt')
