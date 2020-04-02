import numpy as np
import multiprocessing as mp
import matplotlib.pyplot as plt
import operator
import time


def run():
    m = 5
    n = 10
    p = 6
    M = np.random.rand(m, n)
    N = np.random.rand(n, p)

    print("Matrix M: m by n")
    print("Matrix N: n by p")
    print("m = " + str(m))
    print("n = " + str(n))
    print("p = " + str(p))


    beginning_time = time.time()
    P_result = calculate_product_serial_manner(M, N)
    elapsed_time_serial_processing = time.time() - beginning_time
    print('\nSERIAL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_serial_processing) + ' s\n')
    print(P_result)
    beginning_time = time.time()
    P_result = calculate_product_parallel_manner(M, N)
    elapsed_time_parallel_processing = time.time() - beginning_time
    print('\n' + '#' * 25 + '\n')
    print('PARALLEL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_parallel_processing) + ' s\n')
    print(P_result)



def comparision_for_different_matrix_sizes():

    m = 100
    n_list = np.arange(100, 1600, 200)
    p = 120

    serialized_times = []
    parallel_times = []
    for n in n_list:
        M = np.random.rand(m, n)
        N = np.random.rand(n, p)

        beginning_time = time.time()
        P_result = calculate_product_serial_manner(M, N)
        elapsed_time_serial_processing = time.time() - beginning_time
        serialized_times.append(elapsed_time_serial_processing)

        beginning_time = time.time()
        P_result = calculate_product_parallel_manner(M, N)
        elapsed_time_parallel_processing = time.time() - beginning_time
        parallel_times.append(elapsed_time_parallel_processing)

    plt.plot(n_list, serialized_times, 'o-', label='Serialized')
    plt.plot(n_list, parallel_times, 'o-', label='Parallel')
    plt.title(
        r'Running time of $M_{m\times n} \cdot N_{n \times p}$ for different $n$; $m=$' + str(m) + ', $p=$' + str(p))
    plt.xlabel(r'Chunks number ($n$ dimension)')
    plt.ylabel('Time [s]')
    plt.legend()
    plt.grid()
    plt.show()


def calculate_product_serial_manner(M, N):
    map_result_M = []
    for i in range(M.shape[0]):
        for j in range(M.shape[1]):
            result = map_function_M('M', (i, j), M[i][j], N.shape[1])
            map_result_M.append(result)

    map_result_N = []
    for j in range(N.shape[0]):
        for k in range(N.shape[1]):
            result = map_function_N('N', (j, k), N[j][k], M.shape[0])
            map_result_N.append(result)

    # flatening lists:
    map_result_M = [item for sublist in map_result_M for item in sublist]
    map_result_N = [item for sublist in map_result_N for item in sublist]

    result_P = reduce_function(M.shape[0], N.shape[1], map_result_M, map_result_N)
    return result_P


def calculate_product_parallel_manner(M, N):
    pool = mp.Pool(mp.cpu_count() - 1)
    map_result_M = pool.starmap(map_function_M, [('M', (i, j), M[i][j], N.shape[1])
                                                 for i in range(M.shape[0]) for j in range(M.shape[1])])

    map_result_N = pool.starmap(map_function_N, [('N', (j, k), N[j][k], M.shape[0])
                                                 for j in range(N.shape[0]) for k in range(N.shape[1])])

    map_result_M = [item for sublist in map_result_M for item in sublist]
    map_result_N = [item for sublist in map_result_N for item in sublist]

    result_P = reduce_function(M.shape[0], N.shape[1], map_result_M, map_result_N)
    return result_P


def map_function_M(matrix_letter, position, value, N_cols_num):
    i, j = position
    return [((i, k), (matrix_letter, j, value)) for k in range(N_cols_num)]


def map_function_N(matrix_letter, position, value, M_rows_num):
    j, k = position
    return [((i, k), (matrix_letter, j, value)) for i in range(M_rows_num)]


def reduce_function(m, p, map_result_M, map_result_N):
    P = np.zeros(shape=(m, p))
    M_assosiative_lists = {}
    for (i, k), (matrix_letter, j, value) in map_result_M:
        if (i, k) in M_assosiative_lists:
            M_assosiative_lists[(i, k)].append((matrix_letter, j, value))
        else:
            M_assosiative_lists[(i, k)] = [(matrix_letter, j, value)]

    for k, v in M_assosiative_lists.items():
        v.sort(key=operator.itemgetter(1))

    N_assosiative_lists = {}
    for (i, k), (matrix_letter, j, value) in map_result_N:
        if (i, k) in N_assosiative_lists:
            N_assosiative_lists[(i, k)].append((matrix_letter, j, value))
        else:
            N_assosiative_lists[(i, k)] = [(matrix_letter, j, value)]

    for k, v in N_assosiative_lists.items():
        v.sort(key=operator.itemgetter(1))

    for i in range(m):
        for k in range(p):
            vector_M = np.array([value for _, _, value in M_assosiative_lists[(i, k)]])
            vector_N = np.array([value for _, _, value in N_assosiative_lists[(i, k)]])
            P[i][k] = vector_M.dot(vector_N)

    return (P)


if __name__ == "__main__":
    run()
