import os
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
# at the beginning:
start_time = time.time()
sns.set(style="whitegrid")


#results_pickle = os.path.join(os.getcwd(), 'py', 'pickle', 'test_suite_results.p')
results_pickle = os.path.join(os.getcwd(), 'pickle', '100_test_suite_results.p')

with open(results_pickle, 'rb') as fp:
    results_dict = pickle.load(fp)

df = pd.DataFrame.from_dict(results_dict)

'''
Structure of result for each address (row) geocoder (column) pair
[base_result, result with no prefix, result with no state, result with wrong suffix,
 result with no commas, result with no city]
'''

stress_test_list = ['No prefix', 'No state', 'Wrong suffix', 'No commas', 'No city']
geocoder_list = df.columns.values.tolist()
geocoder_list.remove('osm') #temp fix
summary_df = pd.DataFrame(index=geocoder_list, columns=stress_test_list)

for geocoder in geocoder_list:
    for stress_test in stress_test_list:
        results_count = 0
        match_count = 0

        results = df[(df[geocoder].str[0]) > 5][geocoder]
        if len(results.str[0]) < 1:
            continue
        print
        stress_index = stress_test_list.index(stress_test) + 1  # adding 1 because 0 is base case address)
        matches = results[results.str[0] == results.str[stress_index]]
        summary_df[stress_test][geocoder] = len(matches) / float(len(results))

print summary_df


fig = summary_df.plot(kind='bar', figsize=(11, 8))

ax = plt.gca()
for label in ax.get_xticklabels():
    label.set_rotation(45)
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
ax.set_ylabel("Percent of times the challenge address results matched original address results")

# Put a legend to the right of the current axis
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

#plt.show()
plt.savefig('geocoder_precision_comp.png', transparent=False)



plt.show()

# g = sns.factorplot(x="class", y="survived", hue="sex", data=titanic,
#                    size=6, kind="bar", palette="muted")
# g.despine(left=True)
# g.set_ylabels("survival probability")




# fig, ax = plt.subplots()
# rects1 = ax.bar(ind, men_means, width, color='r', yerr=men_std)
#
# women_means = (25, 32, 34, 20, 25)
# women_std = (3, 5, 2, 3, 3)
# rects2 = ax.bar(ind + width, women_means, width, color='y', yerr=women_std)
#
# # add some text for labels, title and axes ticks
# ax.set_ylabel('Scores')
# ax.set_title('Scores by group and gender')
# ax.set_xticks(ind + width / 2)
# ax.set_xticklabels(('G1', 'G2', 'G3', 'G4', 'G5'))
#
# ax.legend((rects1[0], rects2[0]), ('Men', 'Women'))


# def autolabel(rects):
#     """
#     Attach a text label above each bar displaying its height
#     """
#     for rect in rects:
#         height = rect.get_height()
#         ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
#                 '%d' % int(height),
#                 ha='center', va='bottom')
#
# autolabel(rects1)
# autolabel(rects2)

plt.show()


print("%f seconds" % (time.time() - start_time))
