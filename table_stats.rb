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

    @current_user_count = ActiveRecord::Base.connection.select_all("SHOW TABLE STATUS LIKE 'users';")[0]["Rows"].to_i
    @grow_factor = (@new_user_count.to_f / @current_user_count.to_f)

    @io = StringIO.new
  end

public
  # Calculate table, db sizes and estimate future db size (as a function of user count).
  # Returns a report in a string.
  def calculate_sizes
    db_size = 0 # Reported database size
    cdb_size = 0 # Calculated database size
    edb_size = 0 # Estimated db size
    if @print_table_summary
      @io.puts "Table name".ljust(35) + "Size [MB]".rjust(10) + "CSize [MB]".rjust(11) + "Rows [#]".rjust(10) + "Rowlen[B]".rjust(10)
      @io.puts "".ljust(76,"=")
    end

    @tables.each do |table|
      next if exclude_table?(table)

      table_stat = ActiveRecord::Base.connection.select_all("SHOW TABLE STATUS LIKE '#{table}';")[0]
      # Sum up reported table size and reported index size. Doesn't seem to work for MRG_MYISAM tables, hence the calculation below
      table_size = (table_stat['Data_length'].to_i + table_stat['Index_length'].to_i)
      # Another way to calculate table size, multiply rows with average row length and add the index size (seems to give better results)
      ctable_size = (table_stat['Rows'].to_i * table_stat['Avg_row_length'].to_i) + table_stat['Index_length'].to_i

      # Total calculations
      db_size += table_size
      cdb_size += ctable_size
      if(is_user_related_table?(table))
        edb_size += ctable_size * @grow_factor
      else
        edb_size += ctable_size
      end

      # Per table summary
      if @print_table_summary
        @io.puts "#{table}".ljust(35,".") + "#{table_size/1.megabyte}".rjust(10) + "#{ctable_size/1.megabyte}".rjust(10) + "#{table_stat["Rows"]}".rjust(10) + "#{table_stat["Avg_row_length"]}".rjust(10)
      end
    end

    if @print_table_summary
      @io.puts "".ljust(76,"=")
      @io.puts "Totals:"
    end

    @io.puts "  Reported DB size is #{db_size/1.gigabyte}GB (#{@current_user_count} users)."
    @io.puts "  Calculated DB size is #{cdb_size/1.gigabyte}GB."
    if(@new_user_count > @current_user_count)
      @io.puts "  Estimated size is #{edb_size.to_i/1.gigabyte}GB for #{@new_user_count} users."
    end
    @io.string
  end

private
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
