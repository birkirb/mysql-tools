require 'optparse'

def parse_database_options(options = Hash.new)
  options.merge!({
    :host => 'localhost',
    :database => nil,
    :username => nil,
    :password => nil,
  })

  parser = OptionParser.new do |opts|
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

    yield(opts, options)

    opts.on_tail("-i", "--help", "Show this message") do
      puts opts
      exit
    end
  end

  parser.parse!(ARGV)

  options
end

def parse_database_options_with_table_list(options = Hash.new)
  parse_database_options(options) do |opts|
    opts.on("-t", "--tables table_1,table_2", Array,  "List of tables to scan.") do |var|
      options[:tables] = var
    end

    yield(opts, options) if block_given?
  end

  tables = options.delete(:tables)
  [options, tables]
end

def fill_missing_database_parameters(options)
  options.each do |key,value|
    if value.nil?
      print "#{key}: "
      options[key] = STDIN.gets.chomp!
    end
  end
end

def establish_connection!(options)
  ActiveRecord::Base.establish_connection(
    options.merge!({
      :adapter => "mysql2",
    })
  )
end

class Utils
  def self.quote_entity(entity)
    ActiveRecord::Base.connection.quote_column_name(entity)
  end

  def self.quote_string(s)
    ActiveRecord::Base.connection.quote_string(s)
  end

  def self.get_column_definition(table_name, column_name)
    ActiveRecord::Base.connection.send(:column_for, table_name, column_name)
  end

  def self.get_unsigned_primary_key_type(type)
    db_type = ActiveRecord::Base.connection.native_database_types[type.to_sym]
    if db_type
      "#{db_type[:name].to_s}(#{db_type[:limit]}) unsigned default NULL auto_increment"
    else
      raise "Uknown type: #{type.to_s}"
    end
  end

  def self.get_signed_primary_key_type(type)
    db_type = ActiveRecord::Base.connection.native_database_types[type.to_sym]
    if db_type
      "#{db_type[:name].to_s}(#{db_type[:limit]}) default NULL auto_increment"
    else
      raise "Uknown type: #{type.to_s}"
    end
  end
end
