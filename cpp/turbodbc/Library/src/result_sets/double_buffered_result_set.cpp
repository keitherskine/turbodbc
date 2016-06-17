#include <turbodbc/result_sets/double_buffered_result_set.h>

#include <turbodbc/make_description.h>

#include <sqlext.h>

namespace turbodbc { namespace result_sets {

double_buffered_result_set::double_buffered_result_set(std::shared_ptr<cpp_odbc::statement const> statement, std::size_t buffered_rows) :
	statement_(statement),
	active_reading_batch_(0)
{
	auto const rows_per_single_buffer = buffered_rows / 2 + buffered_rows % 2;
	std::size_t const n_columns = statement_->number_of_columns();

	statement_->set_attribute(SQL_ATTR_ROW_ARRAY_SIZE, rows_per_single_buffer);

	for (auto & batch : batches_) {
		for (std::size_t one_based_index = 1; one_based_index <= n_columns; ++one_based_index) {
			auto column_description = make_description(statement_->describe_column(one_based_index));
			batch.columns.emplace_back(*statement, one_based_index, buffered_rows, std::move(column_description));
		}
	}

}

double_buffered_result_set::~double_buffered_result_set() = default;

std::size_t double_buffered_result_set::do_fetch_next_batch()
{
	active_reading_batch_ = (active_reading_batch_ == 0) ? 1 : 0;
	for (auto & column : batches_[active_reading_batch_].columns) {
		column.bind();
	}
	statement_->set_attribute(SQL_ATTR_ROWS_FETCHED_PTR, &(batches_[active_reading_batch_].rows_fetched));
	statement_->fetch_next();
	return batches_[active_reading_batch_].rows_fetched;
}


std::vector<column_info> double_buffered_result_set::do_get_column_info() const
{
	std::vector<column_info> infos;
	for (auto const & column : batches_[active_reading_batch_].columns) {
		infos.push_back(column.get_info());
	}
	return infos;
}


std::vector<std::reference_wrapper<cpp_odbc::multi_value_buffer const>> double_buffered_result_set::do_get_buffers() const
{
	std::vector<std::reference_wrapper<cpp_odbc::multi_value_buffer const>> buffers;
	for (auto const & column : batches_[active_reading_batch_].columns) {
		buffers.push_back(std::cref(column.get_buffer()));
	}
	return buffers;
}


} }
