require 'etl/batch/context'
require 'etl/batch/directive'
require 'etl/batch/run'

module ETL #:nodoc:

  class Batch
    attr_accessor :file
    attr_accessor :engine

    autoload :UseTempTables, 'etl/batch/use_temp_tables'

    # Resolve the given object to an ETL::Control instance. Acceptable arguments
    # are:
    # * The path to a control file as a String
    # * A File object referencing the control file
    # * The ETL::Control object (which will just be returned)
    #
    # Raises a ControlError if any other type is given
    def self.resolve(batch, engine)
      batch = do_resolve(batch)
      batch.engine = engine
      batch
    end

    def self.parse(batch_file)
      batch_file = batch_file.path if File === batch_file

      batch = ETL::Batch.new(batch_file)

      eval(IO.readlines(batch_file).join("\n"), Context.create(batch), batch_file)
      batch
    end

    def self.do_resolve(batch)
      case batch
      when String     then ETL::Batch.parse(File.new(batch))
      when File       then ETL::Batch.parse(batch)
      when ETL::Batch then batch
      else
        raise RuntimeError, "Batch must be a String, File or Batch object"
      end
    end

    def initialize(file)
      @file = file
    end

    def run(file)
      directives << Run.new(self, file)
    end

    def use_temp_tables(value = true)
      directives << UseTempTables.new(self)
    end

    def execute
      engine.say "Executing batch"
      before_execute
      directives.each do |directive|
        directive.execute
      end
      engine.say "Finishing batch"
      after_execute
      engine.say "Batch complete"
    end

    def directives
      @directives ||= []
    end

    def before_execute

    end

    def after_execute
      ETL::Engine.finish # TODO: should be moved to the directive?
      ETL::Engine.use_temp_tables = false # reset the temp tables
    end
  end

end
