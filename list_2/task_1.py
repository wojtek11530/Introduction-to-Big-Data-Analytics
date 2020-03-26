import numpy as np
import time
import multiprocessing as mp
import numpy as np
import matplotlib.pyplot as plt


def run():
    # single_run()
    comparision_for_different_size()


def single_run():
    min_value = 3
    max_value = 7

    n_rows = 1000
    n_cols = 1000
    matrix = get_matrix(n_rows, n_cols)
    beginning_time = time.time()
    result = counting_elements(matrix, min_value, max_value)
    elapsed_time = time.time() - beginning_time
    print('Serialized')
    print('Result: ' + str(result))
    print('Elapsed time: ' + str(elapsed_time) + ' s')

    beginning_time = time.time()
    result = counting_elements_multiprocessing(matrix, min_value, max_value)
    elapsed_time = time.time() - beginning_time
    print('\nParallel (multiprocessing)')
    print('Result: ' + str(result))
    print('Elapsed time: ' + str(elapsed_time) + ' s')


def comparision_for_different_size():
    min_value = 3
    max_value = 7

    exponents_1 = np.arange(1, 5)
    exponents_2 = np.arange(1, 4)

    sizes_1 = np.power([10] * len(exponents_1), exponents_1)
    sizes_2 = 5 * np.power([10] * len(exponents_2), exponents_2)
    sizes_3 = 2 * np.power([10] , 4)
    sizes = np.sort(np.hstack((sizes_1, sizes_2, sizes_3)))

    serialized_times = []
    parallel_times = []
    for n in sizes:
        n_rows = n
        n_cols = n_rows
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
    plt.title('Running time for different matrix size')
    plt.xlabel('Matrix size')
    plt.ylabel('Time [s]')
    plt.xscale('log')
    plt.legend()
    plt.grid()
    plt.show()


def get_matrix(rows_number, columns_number):
    matrix = np.random.randint(0, 10, size=(rows_number, columns_number))
    matrix = matrix.tolist()
    return matrix


def counting(list, min_value, max_value):
    count = 0
    for element in list:
        if min_value <= element and element <= max_value:
            count += 1
    return count


def counting_elements(matrix, min_value, max_value):
    results = []
    for list in matrix:
        results.append(counting(list, min_value, max_value))
    result = sum(results)
    return result


def counting_elements_multiprocessing(matrix, min_value, max_value):
    pool = mp.Pool(mp.cpu_count())
    results = pool.starmap(counting, [(list, min_value, max_value) for list in matrix])
    result = sum(results)
    return result


if __name__ == "__main__":
    run()
