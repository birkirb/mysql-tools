# -*- coding: utf-8 -*-
#!/usr/bin/env ruby

require 'stringio'

# Class to query the database for table stats.
# All of the information is maintained internally by MySQL and all figures are estimates.
# They should give a decent estimate on the order of the table and database sizes.
# Assumes a User table.
class TableStats

  # Pass a new_user_count to get a linear estimate of the DB size for that many users.
  def initialize(print_table_summary = false, new_user_count = 0, user_tables = [], exclude_patterns = [])
    @new_user_count = new_user_count
    @print_table_summary = print_table_summary
    @tables = ActiveRecord::Base.connection.select_values("SHOW TABLES;")
    @user_tables = user_tables.inject({"users" => true }) do |hash, table|
      hash[table] = true
      hash
    end
    @exclude_list = exclude_patterns.map { |regexp| Regexp.new(regexp) }
    @current_user_count = ActiveRecord::Base.connection.select_all(
      "SHOW TABLE STATUS LIKE 'users';")[0]["Rows"].to_i
    @grow_factor = (@new_user_count.to_f / @current_user_count.to_f)
    @io = StringIO.new
  end

  public

  # Calculate table, db sizes and estimate future db size (as a function of user count).
  # Returns a report in a string.
  def calculate_sizes
    reported_db_size   = 0
    calculated_db_size = 0
    estimated_db_size  = 0

    print_header_and_footer do
      @tables.each do |table|
        next if exclude_table?(table)

        table_stat = ActiveRecord::Base.connection.select_all("SHOW TABLE STATUS LIKE '#{table}';")[0]
        # Sum up reported table and index size.
        # Doesn't seem to work for MRG_MYISAM tables, hence the calculation below

        row_count      = table_stat['Rows'].to_i
        index_size     = table_stat['Index_length'].to_i
        table_size     = table_stat['Data_length'].to_i + index_size
        avg_row_size   = table_stat['Avg_row_length'].to_i
        # avg_index_size = row_count > 0 ? index_size/row_count : index_size

        # Another way to calculate table size (seems to give better results)
        calculated_table_size = (row_count * avg_row_size) + index_size

        # Table sums
        reported_db_size += table_size
        calculated_db_size += calculated_table_size
        if(is_user_related_table?(table))
          estimated_db_size += calculated_table_size * @grow_factor
        else
          estimated_db_size += calculated_table_size
        end

        # Per table summary
        if @print_table_summary
          @io.puts "#{table}".ljust(35,".") + "#{table_size/1.megabyte}".rjust(10) +
                   "#{calculated_table_size/1.megabyte}".rjust(10) +
                   "#{index_size/1.megabyte}".rjust(10) +
                   "#{row_count}".rjust(10) +
                   "#{avg_row_size}".rjust(10)
        end
      end
    end

    @io.puts "  Reported DB size is #{reported_db_size/1.gigabyte}GB (#{@current_user_count} users)."
    @io.puts "  Calculated DB size is #{calculated_db_size/1.gigabyte}GB."
    if(@new_user_count > @current_user_count)
      @io.puts "  Estimated size is #{estimated_db_size.to_i/1.gigabyte}GB for #{@new_user_count} users."
    end
    @io.string
  end

  private

  def print_header_and_footer
    line_length = 86
    if @print_table_summary
      @io.puts "Table name".ljust(35) + "Size [MB]".rjust(10) + "CSize [MB]".rjust(11) +
               "Index[MB]".rjust(10) + "Rows [#]".rjust(10) + "Rowlen[B]".rjust(10)
      @io.puts "".ljust(line_length,"=")
    end

    yield

    if @print_table_summary
      @io.puts "".ljust(line_length,"=")
      @io.puts "Totals:"
    end
  end

  # Returns true if and only if table matches the @exclude_list
  def exclude_table?(table)
    @exclude_list.each do |exclude|
      if exclude.match(table)
        return true
      end
    end
    false
  end

  # Returns true if and only if table is marked in @user_tables
  def is_user_related_table?(table)
    return @user_tables[table]
  end
end

if __FILE__ == $0
  require 'active_support'
  require 'active_record'
  load 'database_options_parser.rb'

  options = {
    :exclude => [],
    :new_user_count => 0,
  }
  options, tables = parse_database_options_with_table_list(options) do |opts|
    opts.on("-x", "--exclude REGEXP_1,REGEXP_2", Array, "List of regexpes to exclude tables.") do |var|
      options[:exclude] = var
    end

    opts.on("-c", "--user-count New User Count", "Evaluate DB size increase given this many users.") do |var|
      options[:new_user_count] = var
    end
  end
  fill_missing_database_parameters(options)
  establish_connection!(options)

  ds = TableStats.new(true, options[:new_user_count].to_i, tables, options[:exclude])
  puts ds.calculate_sizes
end
