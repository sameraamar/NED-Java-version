from pprint import pprint
import pandas
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.mlab as mlab

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


def hist(feature, ax, title, bins=100, normed=0, log=False, color="blue"):
    #np_hist(feature, title, ax, color, log=log, bins=bins, normalize=(normed == 1))

    h = ax.hist(feature, bins=bins, histtype='bar', normed=normed, color=color, log=log)
    ax.set_title(title)


#sample histogram: http://matplotlib.org/examples/statistics/histogram_demo_multihist.html

def retweets(events, no_events):
    feature_no_events = no_events['retweets']
    feature_events = events['retweets']

    feature_hist(feature_events, feature_no_events, 'retweets')



def likes(events, no_events):
    feature_no_events = no_events['likes']
    feature_events = events['likes']

    feature_hist(feature_events, feature_no_events, 'likes')


def calcGroups(events, no_events):
    group_no_event = no_events.groupby('group')
    group_event    = events.groupby('group')

    agg_no_events = group_no_event.agg(['count'])
    agg_events    = group_event.agg(['count'])
    feature_hist(agg_events['id']['count'], agg_no_events['id']['count'], 'groups')


    #fig, ax = plt.subplots(figsize=(8, 6))
    #for label, df in groups:
    #    df.vals.plot(kind="kde", ax=ax, label=label)
    #plt.legend()





def feature_hist(feature_events, feature_no_events, feature_name):

    fig, axes = plt.subplots(nrows=3, ncols=2)
    ax0, ax1, ax2, ax3, ax4, ax5 = axes.flatten()

    hist(feature_no_events, ax0, feature_name+' - no events', bins=100, color="red")
    hist(feature_no_events, ax2, feature_name+' (normalized) - no events', normed=1, bins=100, color="red")
    hist(feature_no_events, ax4, feature_name+' (log) - no events', log=True, bins=100, color="red")

    hist(feature_events, ax1, feature_name+' - events', bins=100, color="blue")
    hist(feature_events, ax3, feature_name+' (normalized) - events', normed=1, bins=100, color="blue")
    hist(feature_events, ax5, feature_name+' (log) - events', bins=100, log=True, color="blue")

    # weights = np.ones_like(feature_no_events) / len(feature_no_events)
    # ax2.hist(feature_no_events, bins=100, weights=weights, histtype='bar', color="red")


    fig.tight_layout()
    plt.show()

    print("Plotted features for", feature_name)



if __name__ == "__main__":
    dataset = pandas.read_csv('c:/temp/dataset.txt')
    dataset['jRtwt'] = dataset['jRtwt'].astype(str)

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

#plt.show()