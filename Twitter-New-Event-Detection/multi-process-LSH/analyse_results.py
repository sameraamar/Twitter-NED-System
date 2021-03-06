
import re
import matplotlib.pyplot as plt

import numpy as np



file_name = 'C:\\Users\\t-saaama\\AppData\\Local\\Temp\\LSH\\20170228141115005358_50000\\log_0050000_docs.log'

file = open(file_name, 'r')

print('documents\treported_period\taht\tdimension')


def scatter_plot_with_correlation_line(x, y, graph_filepath, title):
    '''
    http://stackoverflow.com/a/34571821/395857
    x does not have to be ordered.
    '''

    plt.figure()
    # Scatter plot
    plt.scatter(x, y)
    plt.title(title)
    # Add correlation line
    axes = plt.gca()
    m, b = np.polyfit(x, y, 1)
    X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
    plt.plot(X_plot, m*X_plot + b, '-')

    if graph_filepath != None:
        # Save figure
        plt.savefig(graph_filepath, dpi=300, format='png', bbox_inches='tight')

    else:
        plt.show()

    plt.close()


def scatter_plot(x, y, graph_filepath, title):
    '''
    http://stackoverflow.com/a/34571821/395857
    x does not have to be ordered.
    '''

    plt.figure()
    # Scatter plot
    plt.scatter(x, y)
    plt.title(title)

    if graph_filepath != None:
        # Save figure
        plt.savefig(graph_filepath, dpi=300, format='png', bbox_inches='tight')

    else:
        plt.show()

    plt.close()



if __name__ == '__main__':

    documents = []
    reported_periods = []
    ahts = []
    dimensions = []

    for line in file:

        #match = re.search(r"\(AHT: (\d+\.\d+)\(s\)\)", line)
        #if match:
        #    wait = 0
        match = re.search(r'Processed (\d+) documents \(reported in( (\d+) minutes,)* (\d+) seconds\). \(AHT: (\d+\.\d+)\(s\)\). Word vector dimention is (\d+)', line)

        if match:
            documents.append(match.group(1))
            p = float(match.group(4))/60
            if match.group(3) != None:
                p += int(match.group(3))
            reported_periods.append(p)

            ahts.append(match.group(5))
            dimensions.append( match.group(6))

                    #print('{0}\t{1}\t{2}\t{3}'.format(doc, per, aht, dim))

    file.close()

    scatter_plot(documents, ahts, None, "AHT per Documents")
    scatter_plot(documents, dimensions, None, "Dimension per Documents")
    scatter_plot(documents, reported_periods, None, "Reported Period per Documents")

