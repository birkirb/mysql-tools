# -*- coding: utf-8 -*-
#!/usr/bin/env ruby

# This classes analyzes a MySQL schema in relation to it's data,
# and suggest possible improvements.
class MysqlSchemaAnalyzer

  REPORT_CHAR_USAGE_UNDER = 0.65 # Report varchars that in the maximum case uses less than this percentage of defined length
  REPORT_CHAR_AVERAGE_UNDER = 0.30 # Report varchars where the average size is less than this percentage of the defined size
  MULTIPLE_INDEX_THRESHOLD_SIZE = 4

  def initialize(tables = nil)
    env = ActiveRecord::Base.connection.instance_variable_get('@config').dup
    @db = env[:database]
    @host = env[:host]
    @tables = tables
    @type_keys = [:tinyint, :smallint, :mediumint, :int, :bigint]
    @types = {:tinyint   => { :maxsize => 255,   :factor => 1,},
              :smallint  => { :maxsize => 65535, :factor => 10 },
              :mediumint => { :maxsize => 16777215, :factor => 100 },
              :int       => { :maxsize => 4294967295, :factor => 10 },
              :bigint    => { :maxsize => 18446744073709551615, :factor => 1 }}
  end

  def print_indices?
    true
  end

  def print_stats
    db_tables = ActiveRecord::Base.connection.tables
    tables = (@tables & db_tables) || db_tables

    if tables.blank?
      puts "No tables to scan."
      return
    end

    title = " ANALYSING SCHEMA: #{@db.upcase} @ #{@host.upcase} "
    puts "".ljust(5,'*') + title + "".ljust(75 - title.size,'*')

    tables.each do |table_name|
      table_summary_lines = []

      record_count = ActiveRecord::Base.connection.select_all("SHOW TABLE STATUS LIKE '#{table_name}';")[0]["Rows"].to_i

      sql = <<-SQL
        SELECT table_name,column_name,column_type, data_type,character_maximum_length, numeric_precision
          FROM information_schema.`columns`
          WHERE `table_name` = '#{table_name}' AND `table_schema` = '#{@db}'
      SQL

      columns = ActiveRecord::Base.connection.select_all(sql)
      columns_size = columns.size

      columns.each do |column|
        column_summary_lines = []

        column_type    = column['data_type']
        column_name    = column['column_name']
        defined_length = column['character_maximum_length'].to_i
        is_char        = (column_type == 'char')
        is_varchar     = (column_type == 'varchar')
        is_int         = (column_type.index('int') != nil)

        if record_count && record_count > 0
          column_summary_lines += char_checks(table_name, column)
          column_summary_lines += int_checks(table_name, column)
        end

        if column_summary_lines.size > 0
          column_definition = ActiveRecord::Base.connection.select_all("SHOW CREATE TABLE #{@db}.#{table_name};")[0]["Create Table"].split("\n").grep(Regexp.new("`#{column_name}` ")).first.to_s.strip
          table_summary_lines << "".ljust(4) + "Column: #{column_name} #{column_type}(#{defined_length})"
          table_summary_lines << "".ljust(4) + "Definition: #{column_definition}"
          table_summary_lines += column_summary_lines
        end
      end

      sql = <<-SQL
        SELECT
          table_name AS `table`,
          index_name AS `index`,
          cardinality,
          GROUP_CONCAT(column_name ORDER BY seq_in_index) AS `columns`
        FROM information_schema.`statistics`
        WHERE `table_name` = '#{table_name}' AND `table_schema` = '#{@db}'
        GROUP BY 1,2
      SQL

      indices = ActiveRecord::Base.connection.select_all(sql)
      index_count = 0
      multiple_index_count = 0
      index_summary_lines = []

      indices.each do |index|
        index_count +=1
        multiple_index_count += index['columns'].split(',').size
        index_summary_lines.push("".ljust(8) + "#{index['index'] == 'PRIMARY' ? 'PRIMARY ' : ''}#{index['columns']}(#{index['cardinality']})")
      end

      if table_summary_lines.size > 0
        puts "".ljust(2) + "== TABLE REPORT ".ljust(78, '=')
        puts "".ljust(2) + "Name: #{table_name}"
        puts "".ljust(2) + "Row count: #{record_count}, Column count: #{columns_size}"
        puts "".ljust(2) + "Index count: #{index_count}, With multiple: #{multiple_index_count}"
        puts "".ljust(2) + "TOO MANY INDICES!" if (columns_size > MULTIPLE_INDEX_THRESHOLD_SIZE && multiple_index_count > columns_size)
        puts "".ljust(2) + "POSSIBLY UNUSED TABLE" unless (record_count && record_count > 0)

        puts table_summary_lines.join("\n")
        if print_indices?
          puts "".ljust(4) + "Indices:"
          puts index_summary_lines.sort.join("\n")
        end
      end
    end
    puts "".ljust(80,'*')
  end

  def int_checks(table_name, column)
    column_summary_lines = []
    data_type      = column['data_type']
    column_type    = column['column_type']
    column_name    = column['column_name']
    is_int         = (column_type.index('int') != nil)
    is_signed      = !column_type.index('unsigned')

    return column_summary_lines unless is_int

    minimum_value = ActiveRecord::Base.connection.select_value("SELECT MIN(`#{column_name}`) AS number FROM `#{@db}`.`#{table_name}`;")
    maximum_value = ActiveRecord::Base.connection.select_value("SELECT MAX(`#{column_name}`) AS number FROM `#{@db}`.`#{table_name}`;")

    if column_type == 'tinyint(1)'
      # Boolean type
      if maximum_value == minimum_value
        column_summary_lines << "".ljust(8) + "POSSIBLY UNUSED BOOLEAN (TINYINT)"
      end
    elsif maximum_value == nil || maximum_value == 0
        column_summary_lines << "".ljust(8) + "POSSIBLY UNUSED INTEGER"
    else
      if minimum_value && minimum_value > -1 && is_signed
        column_summary_lines << "".ljust(8) + "POSSIBLE UNSIGNED INTEGER CANDIDATE"
      end

      @type_keys.each do |key|
        if ((maximum_value * @types[key][:factor]) < @types[key][:maxsize])
          if data_type != key.to_s
            column_summary_lines << "".ljust(8) + "POSSIBLE #{key.to_s.upcase} CANDIDATE"
            break
          else
            if maximum_value < (@types[key][:maxsize] / @types[key][:factor])
              # Good fit
              break
            end
          end
        end
      end

      if column_type.index('bigint').nil? && (maximum_value * 3) > @types[:int][:maxsize]
        column_summary_lines << "".ljust(8) + "POSSIBLY UNDERSIZED INT, BIGINT CANDIDATE"
      end
    end

    if column_summary_lines.size > 0
      column_summary_lines << "".ljust(10) + "Maximum value: #{maximum_value}"
      column_summary_lines << "".ljust(10) + "Minimum value: #{minimum_value}"
    end

    column_summary_lines
  end

  def char_checks(table_name, column)
    lines = []
    column_type    = column['data_type']
    column_name    = column['column_name']
    defined_length = column['character_maximum_length'].to_i
    is_char        = (column_type == 'char')
    is_varchar     = (column_type == 'varchar')

    return lines unless is_varchar || is_char

    # TODO: Check median here instead, std dev might be userful as well
    average_length = ActiveRecord::Base.connection.select_value("SELECT AVG(CHAR_LENGTH(`#{column_name}`)) AS number FROM `#{@db}`.`#{table_name}`;").to_i
    maximum_length = ActiveRecord::Base.connection.select_value("SELECT MAX(CHAR_LENGTH(`#{column_name}`)) AS number FROM `#{@db}`.`#{table_name}`;").to_i
    line_checks = []

    if is_char
      if maximum_length != defined_length
        line_checks << "".ljust(8) + "POSSIBLY OVERSIZED CHAR"
      end
    else
      if maximum_length == average_length && maximum_length < defined_length
        line_checks << "".ljust(8) + "POSSIBLY OVERSIZED FIXED VALUE VARCHAR"
      end

      if maximum_length < defined_length * REPORT_CHAR_USAGE_UNDER
        line_checks << "".ljust(8) + "POSSIBLY OVERSIZED VARCHAR"
      end

      if average_length < defined_length * REPORT_CHAR_AVERAGE_UNDER
        # REPORT SMALL AVERAGE LENGHT, CHECK INDEXES?
        # TODO: Check if indexed
        # line_checks << "".ljust(8) + "POSSIBLY OVERSIZED INDEX"
      end
    end
    if line_checks.size > 0
      lines << line_checks
      lines << "".ljust(10) + "Average length: #{average_length}"
      lines << "".ljust(10) + "Maximum length: #{maximum_length}"
    end
    lines
  end
end

if __FILE__ == $0
  require 'active_support'
  require 'active_record'
  require 'optparse'

  options = {
    :host => 'localhost',
    :database => nil,
    :username => nil,
    :password => nil,
  }

  OptionParser.new do |opts|
    opts.banner = "Usage: rails #{__FILE__} [options]"

    opts.on("-h", "--host hostname", "MySQL Server Hostname") do |var|
      options[:host] = var
    end

    opts.on("-d", "--database database", "Database to scan") do |var|
      options[:database] = var
    end

    opts.on("-u", "--username username", "Username to connect as") do |var|
      options[:username] = var
    end

    opts.on("-p", "--password password", "User's password") do |var|
      options[:password] = var
    end

    opts.on("-t", "--tables table_1,table_2", Array,  "User's password") do |var|
      options[:tables] = var
    end

    opts.on_tail("-h", "--help", "Show this message") do
      puts opts
      exit
    end
  end.parse!(ARGV)

  tables = options.delete(:tables)

  options.each do |key,value|
    if value.nil?
      print "#{key}: "
      options[key] = gets.chomp!
    end
  end

  ActiveRecord::Base.establish_connection(
    options.merge!({
      :adapter => "mysql2",
    })
  )

  MysqlSchemaAnalyzer.new(tables).print_stats
end
