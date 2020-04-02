import numpy as np
import time
import multiprocessing as mp
import numpy as np
import matplotlib.pyplot as plt


def run():
    single_run()
    #comparision_for_different_size()


def single_run():
    min_value = 3
    max_value = 7

    n_rows = 100000
    n_cols = 250
    print("n_rows = " + str(n_rows))
    print("n_cols = " + str(n_cols))
    print("min_value = " + str(min_value))
    print("max_value = " + str(max_value))

    matrix = get_matrix(n_rows, n_cols)
    beginning_time = time.time()
    result = counting_elements(matrix, min_value, max_value)
    elapsed_time_serial_processing = time.time() - beginning_time
    print('\nSerialized')
    print('Result: ' + str(result))
    print('Elapsed time: ' + str(elapsed_time_serial_processing) + ' s')

    beginning_time = time.time()
    result = counting_elements_multiprocessing(matrix, min_value, max_value)
    elapsed_time_parallel_processing = time.time() - beginning_time
    print('\nParallel (multiprocessing)')
    print('Result: ' + str(result))
    print('Elapsed time: ' + str(elapsed_time_parallel_processing) + ' s')


def comparision_for_different_size():
    min_value = 3
    max_value = 7

    n_cols = 250

    exponents = np.arange(1, 7)

    sizes_1 = np.power([10] * len(exponents), exponents)
    sizes_2 = 5 * np.power([10] * len(exponents[:-1]), exponents[:-1])
    sizes = np.sort(np.hstack((sizes_1, sizes_2)))

    serialized_times = []
    parallel_times = []
    for n in sizes:
        n_rows = n
        matrix = get_matrix(n_rows, n_cols)

        beginning_time = time.time()
        counting_elements(matrix, min_value, max_value)
        elapsed_time = time.time() - beginning_time
        serialized_times.append(elapsed_time)

        beginning_time = time.time()
        counting_elements_multiprocessing(matrix, min_value, max_value)
        elapsed_time = time.time() - beginning_time
        parallel_times.append(elapsed_time)

    plt.plot(sizes, serialized_times, 'o-', label='Serialized')
    plt.plot(sizes, parallel_times, 'o-', label='Parallel')
    plt.title('Running time for different matrix sizes, columns number=' + str(n_cols))
    plt.xlabel('Chunks number (rows number)')
    plt.ylabel('Time [s]')
    plt.xscale('log')
    plt.legend()
    plt.grid()
    plt.show()


def get_matrix(rows_number, columns_number):
    matrix = np.random.randint(0, 10, size=(rows_number, columns_number))
    matrix = matrix.tolist()
    return matrix


def counting_mapper(list, min_value, max_value):
    count = 0
    for element in list:
        if min_value <= element and element <= max_value:
            count += 1
    return count

def reduce(mapped_counts_list):
    return sum(mapped_counts_list)


def counting_elements(matrix, min_value, max_value):
    results = []
    for list in matrix:
        results.append(counting_mapper(list, min_value, max_value))
    result = reduce(results)
    return result


def counting_elements_multiprocessing(matrix, min_value, max_value):
    pool = mp.Pool(mp.cpu_count()-1)
    results = pool.starmap(counting_mapper, [(list, min_value, max_value) for list in matrix])
    result = reduce(results)
    return result


if __name__ == "__main__":
    run()
