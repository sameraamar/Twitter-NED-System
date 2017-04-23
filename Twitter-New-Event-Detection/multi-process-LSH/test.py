
words = ['ggg', 'samer', 'mariam',  'why', 'why', 'mariam', 'samer' , 'mariam']

word_counts = {}
for w in words:
    word_counts[w] = word_counts.get(w, 0) + 1

temp = sorted(word_counts, key=lambda x: word_counts[x], reverse=True)

print (word_counts)
print (temp)

text = ' '.join(temp)
print (text)