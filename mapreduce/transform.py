
def transform(file):
    with open('transformed_f.txt', 'w') as outfile:
        for line in infile:
            vals = line.strip().split('\t')
            month = vals[0].split(',')[0].strip('[')
            weekstatus = vals[0].split(',')[1]
            daystatus = vals[0].split(',')[2].strip(']')
            triprev = vals[1]
            outfile.write(month + '\t' + weekstatus + '\t' + daystatus + '\t' + triprev + '\n')


if __name__ == "__main__":
    infile = open('outputs/out_f.txt')
    transform(infile)

