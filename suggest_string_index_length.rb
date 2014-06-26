# -*- coding: utf-8 -*-
class IndexLengthAnalyzer

  def initialize(verbose = true)
    @verbose = verbose
  end

  def suggest_string_index_length(table, column, cut_off_rate = 99.9)
    fine_tuning = false

    qcolumn = Utils.quote_entity(column)
    qtable = Utils.quote_entity(table)

    type = con.select_all("DESCRIBE #{qtable} #{qcolumn}")[0]['Type']
    column_size_check = type.match(/\d+/)
    return "Column #{qcolumn} must have a variable length." if column_size_check.nil?

    column_length = column_size_check[0].to_i
    row_count = con.select_value("SELECT COUNT(*) FROM #{qtable} WHERE #{qcolumn} IS NOT NULL").to_i
    max_value_length = con.select_value("SELECT MAX(CHARACTER_LENGTH(#{qcolumn})) FROM #{qtable} WHERE #{qcolumn} IS NOT NULL").to_i
    column_cardinality = select_cardinality(qtable, qcolumn, max_value_length, true);
    puts "Calculating ideal character length for index on column #{qcolumn} in table #{qtable}. \n" +
      "Column length is #{column_length}. Maximum value length is #{max_value_length}.\n" +
      "Cut off rate set at #{cut_off_rate}%. " +
      "Column cardinality is #{column_cardinality}. " +
      "Rows with values: #{row_count}." if @verbose
    puts "="*80 if @verbose

    previous_test_index_size = max_value_length
    test_index_size = previous_test_index_size/2
    previous_hit_rate = nil

    while test_index_size > 0
      cardinality = select_cardinality(qtable, qcolumn, test_index_size)
      hit_rate = cardinality.to_f/column_cardinality * 100
      puts "Index length of #{test_index_size.to_s.rjust(column_length.to_s.size)} " +
        "has cardinality #{cardinality.to_s.rjust(row_count.to_i.to_s.size)}, " +
        "#{hit_rate.round(2).to_s.rjust(5)}% uniqueness." if @verbose

      if fine_tuning
        if hit_rate < cut_off_rate
          return previous_test_index_size
        end
        previous_test_index_size = test_index_size
        test_index_size -= 1
      else
        if hit_rate < cut_off_rate && column_cardinality != cardinality
          fine_tuning = true
          puts "Fine tuning." if @verbose
          test_index_size = previous_test_index_size - 1
        else
          previous_test_index_size = test_index_size
          test_index_size /= 2
        end
      end
    end
    previous_test_index_size
  end

  private

  def con
    @con ||= ActiveRecord::Base.connection
  end

  def select_cardinality(qtable, qcolumn, with_character_count = 255, count_nils = false)
    where_nils = count_nils ? '' : "WHERE #{qcolumn} IS NOT NULL"
    sql = <<-SQL
        SELECT COUNT(*) FROM
        (
          SELECT COUNT(LEFT(#{qcolumn}, #{with_character_count })) AS count
          FROM #{qtable}
          #{where_nils}
          GROUP BY LEFT(#{qcolumn}, #{with_character_count})
        ) AS tmp
    SQL
    con.select_value(sql).to_i
  end
end

if __FILE__ == $0
  require 'bundler'
  require 'bundler/setup'
  require 'active_support'
  require 'active_record'
  load 'database_options_parser.rb'

  options = {
    :verbose => true,
    :rate => 99.9,
  }
  options = parse_database_options(options) do |opts|
    opts.on("-t", "--table ", "Table to look at") do |var|
      options[:table] = var
    end

    opts.on("-c", "--column ", "Column to calculate for.") do |var|
      options[:column] = var
    end

    opts.on("-r", "--rate ", "Cardinality rate to aim for.") do |var|
      options[:rate] = var
    end

    opts.on("-v", "--verbose ", "Verbose output") do |var|
      options[:verbose] = var
    end
  end
  fill_missing_database_parameters(options)
  establish_connection!(options)

  IndexLengthAnalyzer.new(options[:verbose]).suggest_string_index_length(
    options[:table],
    options[:column],
    options[:rate]
  )
end
