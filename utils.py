import os
import sys

def replace_word(infile, outfile, old_word, new_word):
    if not os.path.isfile(infile):
        print("Error on replace_word, not a regular file: " + infile)
        sys.exit(1)
    f1 = open(infile, 'r').read()
    f2 = open(outfile, 'w')
    m = f1.replace(old_word, new_word)
    f2.write(m);

#replace_word("sample_input.txt", "sample.csv", "^&^", ",")