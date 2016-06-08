#include <iostream>

#include "utils/time/timer.hpp"

using std::cout;
using std::endl;

using ns = std::chrono::nanoseconds;
using ms = std::chrono::milliseconds;

constexpr long iterations_no = 10 * 1024 * 1024;

struct DataShort
{
    long a;
    long b;
};

struct DataLong
{
    long a;
    long b;
    long c;
    long d;
};

DataShort *data_short = new DataShort[iterations_no];
DataLong *data_long = new DataLong[iterations_no];

int main()
{
    auto time_short = timer<ms>([]() {
		for (long i = 0; i < iterations_no; i++) {
			data_short[i].a = data_short[i].b;
		}
    });

	auto time_long = timer<ms>([]() {
		for (long i = 0; i < iterations_no; i++) {
			data_long[i].a = data_long[i].b;
		}
	});

	cout << "Time short: " << time_short << " ms"<< endl;
	cout << "Time long: " << time_long << " ms" << endl;

	return 0;
}
