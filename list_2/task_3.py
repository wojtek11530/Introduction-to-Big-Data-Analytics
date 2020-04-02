import time
import multiprocessing as mp
import numpy as np
import matplotlib.pyplot as plt


def run():
    single_run()
    #comparision_for_different_digitss_number()


def single_run():
    digits_number = 30
    beginning_time = time.time()
    pi_string = determine_pi_digits_serial_manner(digits_number)
    elapsed_time_serial_processing = time.time() - beginning_time

    print('Digits number: ' + str(digits_number))
    print('SERIAL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_serial_processing) + ' s\n')
    print(pi_string)
    beginning_time = time.time()
    pi_string = determine_pi_digits_parallel_manner(digits_number)
    elapsed_time_parallel_processing = time.time() - beginning_time
    print('\n' + '#' * 25 + '\n')
    print('PARALLEL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_parallel_processing) + ' s\n')
    print(pi_string)


def comparision_for_different_digitss_number():
    digits_number = np.arange(100, 2500, 100)

    serialized_times = []
    parallel_times = []
    for n in digits_number:
        beginning_time = time.time()
        pi_string = determine_pi_digits_serial_manner(n)
        elapsed_time_serial_processing = time.time() - beginning_time
        serialized_times.append(elapsed_time_serial_processing)

        beginning_time = time.time()
        pi_string = determine_pi_digits_parallel_manner(n)
        elapsed_time_parallel_processing = time.time() - beginning_time
        parallel_times.append(elapsed_time_parallel_processing)

    plt.plot(digits_number, serialized_times, 'o-', label='Serialized')
    plt.plot(digits_number, parallel_times, 'o-', label='Parallel')
    plt.title(r'Running time for different digits number of $\pi$')
    plt.xlabel('Chunks number (digits number)')
    plt.ylabel('Time [s]')
    plt.legend()
    plt.grid()
    plt.show()


def determine_pi_digits_serial_manner(digits_number):
    digits = []
    for d in range(digits_number):
        digits.append(dth_digit(d))
    pi_string = pi_reduce(digits)
    return pi_string


def determine_pi_digits_parallel_manner(digits_number):
    pool = mp.Pool(mp.cpu_count() - 1)
    digits = pool.map(dth_digit, [d for d in range(digits_number)])
    pi_string = pi_reduce(digits)
    return pi_string


def dth_digit(d):
    # map function
    d -= 1
    x = (4 * S(1, d) - 2 * S(4, d) - S(5, d) - S(6, d)) % 1.0
    return '{:x}'.format(int(x * 16))


def pi_reduce(digits):
    pi_string = digits[0] + '.'
    for i in range(1, len(digits)):
        pi_string += digits[i]
    return pi_string


def S(j, n):
    # Left sum
    s = 0.0
    k = 0
    while k <= n:
        r = 8 * k + j
        s = (s + pow(16, n - k, r) / r) % 1.0
        k += 1
    # Right sum
    t = 0.0
    k = n + 1
    while 1:
        newt = t + pow(16, n - k) / (8 * k + j)
        # Iterate until t no longer changes
        if t == newt:
            break
        else:
            t = newt
        k += 1
    return s + t


if __name__ == '__main__':
    run()
