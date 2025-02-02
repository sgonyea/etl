module ETL #:nodoc:
  class Batch

    class Context
      attr_reader :batch

      # Create a context that is used when evaluating the batch file
      def self.create(batch)
        Context.new(batch)
      end

      def initialize(batch)
        @batch = batch
      end

      def file
        batch.file
      end

      def run(file)
        batch.run(File.dirname(self.file) + "/" + file)
      end

      def use_temp_tables(value=true)
        batch.use_temp_tables(value)
      end

    end

  end
end
