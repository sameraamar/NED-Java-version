import matplotlib.pyplot as plt
import numpy as np
import powerlaw

mu, sigma = 0, 100
x = mu + sigma * np.random.rand(100)


#x= np.log10(x)
bins = 30 #[0, 40, 60, 75, 90, 110, 125, 140, 160, 200]
hist, bins = np.histogram(x, bins=bins)
width = np.diff(bins)
center = (bins[:-1] + bins[1:]) / 2

fig, ax = plt.subplots(figsize=(8,3))
ax.bar(center, hist, align='center', width=width)
ax.set_xticks(bins)
#fig.savefig("/tmp/out.png")

plt.show()