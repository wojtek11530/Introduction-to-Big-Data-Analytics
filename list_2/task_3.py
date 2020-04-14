import time
import multiprocessing as mp
import numpy as np
import matplotlib.pyplot as plt


def run():
    single_run()
    # comparision_for_different_digitss_number()


def single_run():
    position = 0
    digits_number = 30
    beginning_time = time.time()
    pi_string = determine_pi_digits_from_position_serial_manner(digits_number, position)
    elapsed_time_serial_processing = time.time() - beginning_time

    print('Digits number: ' + str(digits_number))
    print('From position: ' + str(position))
    print('SERIAL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_serial_processing) + ' s\n')
    print(pi_string)
    beginning_time = time.time()
    pi_string = determine_pi_digits_from_position_parallel_manner(digits_number, position)
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
        pi_string = determine_pi_digits_from_position_serial_manner(n)
        elapsed_time_serial_processing = time.time() - beginning_time
        serialized_times.append(elapsed_time_serial_processing)

        beginning_time = time.time()
        pi_string = determine_pi_digits_from_position_parallel_manner(n)
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


def determine_pi_digits_from_position_serial_manner(digits_number, beg_position=0):
    digits = []
    for d in range(digits_number):
        digits.append(dth_digit_of_pi(beg_position + d))
    pi_string = pi_reduce(digits, beg_position)
    return pi_string


def determine_pi_digits_from_position_parallel_manner(digits_number, position=0):
    pool = mp.Pool(mp.cpu_count() - 1)
    digits = pool.map(dth_digit_of_pi, [position + d for d in range(digits_number)])
    pi_string = pi_reduce(digits, position)
    return pi_string


def dth_digit_of_pi(d):
    # map function
    d -= 1
    digit = (4 * S(1, d) - 2 * S(4, d) - S(5, d) - S(6, d)) % 1.0
    return d+1, '{:x}'.format(int(digit * 16))


def pi_reduce(digits, position):
    digits = sorted(digits, key=lambda x: x[0])
    pi_string = digits[0][1]
    if position == 0:
        pi_string += '.'
    for i in range(1, len(digits)):
        pi_string += digits[i][1]
    return pi_string


def S(j, d):
    left_sum = 0.0
    k = 0
    while k <= d:
        left_sum = (left_sum + ((16 ** (d - k)) % (8 * k + j)) / (8 * k + j)) % 1.0
        k += 1

    right_sum = 0.0
    k = d + 1
    while True:
        new_right_sum = right_sum + 16 ** (d - k) / (8 * k + j)
        if right_sum == new_right_sum:
            break
        else:
            right_sum = new_right_sum
        k += 1
    return (left_sum + right_sum) % 1


if __name__ == '__main__':
    run()
