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

    opts.on_tail("-h", "--help", "Show this message") do
      puts opts
      exit
    end
  end

  parser.parse!(ARGV)

  options
end

def parse_database_options_with_table_list(options = Hash.new)
  parse_database_options(options) do |opts|
    opts.on("-t", "--tables table_1,table_2", Array,  "User's password") do |var|
      options[:tables] = var
    end

    yield(opts, options)
  end

  tables = options.delete(:tables)
  [options, tables]
end

def fill_missing_database_parameters(options)
  options.each do |key,value|
    if value.nil?
      print "#{key}: "
      options[key] = gets.chomp!
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
