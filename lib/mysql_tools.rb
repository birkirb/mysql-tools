require 'bundler'
require 'bundler/setup'
require 'active_support'
require 'active_record'

module MysqlTools
  load 'database_options_parser.rb'

  options, tables = parse_database_options_with_table_list do |opts, options|
    opts.on("-o", "--output output_directory", "Directory to which the schema file is saved.") do |var|
      options[:output] = var
    end
  end
  fill_missing_database_parameters(options)
  establish_connection!(options)

end
