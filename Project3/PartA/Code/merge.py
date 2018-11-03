import glob
files = glob.glob("partA/input/*.txt")

with open('partA/input/mergefile.txt', 'w') as f:
        for file in files:
                with open(file) as infile:
                        f.write(infile.read()+'\n')
                        
f.close()
