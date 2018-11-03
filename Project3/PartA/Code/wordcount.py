def main():
        outputFile = open("partA/wordcount.txt", "w")
        word_dict = {}
        with open("partA/input/mergefile.txt") as file:
                lines = file.readlines()
                for line in lines:
                        for word in line.split():
                                if word not in word_dict:
                                        word_dict[word] = 1
                                else:
                                        word_dict[word] += 1
        word_sorted_count = sorted(word_dict, key=lambda x:(-word_dict[x],x))

        for word in word_sorted_count:
                outputFile.write(str(word) + '\t' + str(word_dict[word]) + '\n')
        outputFile.close()

if __name__ == "__main__":
        main()
