AutoComplete Project
Implemented MapReduce jobs to construct N-Gram Library from Wiki data and build a statistic language model to construct data for ajax auto complete query search

Language Model: probability distribution over entire sentences

1.MR: N-gram library builder
M: split sentence into N gram and store
I love big data
I , Love, big, data, i love, love big…
key: n gram value: 1
R: for the same key, sum up values


2.MR: Build language Model: calculate probability of a word appear after a phrase, and do threshold check
M: Key: starting phrase Value: following word with count
i love big , data = 10
R: key: starting word, following word, probably write to DB
apply topK filtering on following word for the same starting word