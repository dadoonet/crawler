#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

# frozen_string_literal: true

module Crawler
  class DocumentMapper
    CRAWLER_DOC_SCHEMA = {
      'url' => 'string',

      # URL components
      'url_scheme' => 'string',
      'url_host' => 'string',
      'url_port' => 'string',
      'url_path' => 'string',
      'url_path_dir1' => 'string',
      'url_path_dir2' => 'string',
      'url_path_dir3' => 'string',

      'last_crawled_at' => 'date',
      'title' => 'string',
      'body_content' => 'string',
      'meta_keywords' => 'string',
      'meta_description' => 'string',
      'links' => 'string',
      'headings' => 'string'
    }.freeze

    attr_reader :config

    def initialize(config)
      @config = config
    end

    def document_fields(crawl_result)
      main_components(crawl_result)
        .merge(url_components(crawl_result.url))
        .merge(css_components(crawl_result))
    end

    def main_components(crawl_result)
      remove_empty_values(
        'title' => crawl_result.document_title(limit: config.max_title_size),
        'body_content' => crawl_result.document_body(limit: config.max_body_size),
        'meta_keywords' => crawl_result.meta_keywords(limit: config.max_keywords_size),
        'meta_description' => crawl_result.meta_description(limit: config.max_description_size),
        'links' => crawl_result.links(limit: config.max_indexed_links_count),
        'headings' => crawl_result.headings(limit: config.max_headings_count),
        'last_crawled_at' => crawl_result.start_time&.rfc3339
      )
    end

    def url_components(url)
      url = Crawler::Data::URL.parse(url.to_s) unless url.is_a?(Crawler::Data::URL)
      path_components = url.path.split('/')
      remove_empty_values(
        'url' => url.to_s,
        'url_scheme' => url.scheme,
        'url_host' => url.host,
        'url_port' => url.inferred_port,
        'url_path' => url.path,
        'url_path_dir1' => path_components[1], # [0] is always empty since path starts with a /
        'url_path_dir2' => path_components[2],
        'url_path_dir3' => path_components[3]
      )
    end

    def css_components(crawl_result)
      @config.system_logger.info(@config.extraction_rules)
      @config.system_logger.info(crawl_result.base_url)
      @config.system_logger.info(@config.extraction_rules[crawl_result.base_url])
      @config.system_logger.info(crawl_result.site_url.to_s)
      @config.system_logger.info(@config.extraction_rules[crawl_result.site_url.to_s])

      @config.extraction_rules[crawl_result.base_url]&.each do |_rules|
        @config.system_logger.info('Found a rule!')
        @config.system_logger.info(_rules)
        # crawl_result.extract_by_selector
      end
      {}
    end

    private

    # Accepts a hash and removes empty values from it
    def remove_empty_values(hash_object)
      hash_object.tap { |h| h.reject! { |_, value| value.blank? } }
    end
  end
end
