# -*- coding: utf-8 -*-
#!/usr/bin/env ruby
require 'singleton'

class MysqlCommand
  include Singleton

  def initialize
    env = ActiveRecord::Base.connection.instance_variable_get('@config').dup
    @mysql_command = "mysql -h #{env[:host]} -u #{env[:username]} -p#{env[:password]} -D #{env[:database]} -t"
  end

  def execute(command)
    `#{@mysql_command} --execute=\"#{command}\"`
  end
end

class TableProcessor

  attr_reader :tables

  def initialize(tables = nil)
    db_tables = ActiveRecord::Base.connection.tables
    @tables = (tables & db_tables) || db_tables
  end

  def run(directory)
    tables.each do |table|
      begin
        schema = TableSchema.new(table, directory)
        schema.update
      rescue => err
        STDERR.puts "Failed on table '#{table}'  with: #{err.message}."
        STDERR.puts err.backtrace.join("\n")
      end
    end
  end

end

class TableSchema

  SCHEMA_REGEXP = /^Schema:.*\nNotes:\n/m
  INDEX_REGEXP = /((?:PRIMARY )?KEY.*)\) ENGINE/m
  OUTPUT_DIRECTORY = "schema"

  attr_reader :name, :output_directory

  def initialize(table_name, output_directory = nil)
    @output_directory = output_directory || OUTPUT_DIRECTORY
    @name = table_name
  end

  def description
    execute("DESCRIBE \\`#{name}\\`")
  end

  def indices
    create_statement = execute("SHOW CREATE TABLE \\`#{name}\\`")
    if indices = INDEX_REGEXP.match(create_statement)
      indices.captures.first.split("\n").map do |line|
        "  #{line.strip.gsub(/,$/,'')}"
      end.join("\n")
    end
  end

  def template
    <<-SCHEMA
Schema: #{name}

#{description}
Indices:
#{indices}

Notes:
    SCHEMA
  end

  def update
    existing_schema = read
    new_schema = template
    write(existing_schema.gsub(SCHEMA_REGEXP, new_schema))
  end

  private

  def blank_template
    <<-SCHEMA
Schema:

Notes:
    SCHEMA
  end

  def read
    if File.exist?(file_name)
      File.new(file_name).read #lines.join
    else
      blank_template
    end
  end

  def write(output)
    File.open(file_name, 'w') do |f|
      f.write(output)
    end
  end

  def file_name
    File.join(output_directory, "#{name}.txt")
  end

  def execute(command)
    MysqlCommand.instance.execute(command)
  end
end

if __FILE__ == $0
  require 'active_support'
  require 'active_record'
  load 'database_options_parser.rb'

  options, tables = parse_database_options_with_table_list do |opts, options|
    opts.on("-o", "--output output_directory", "Directory to which the schema file is saved.") do |var|
      options[:output] = var
    end
  end
  fill_missing_database_parameters(options)
  establish_connection!(options)

  TableProcessor.new(tables).run(options[:output])
end
