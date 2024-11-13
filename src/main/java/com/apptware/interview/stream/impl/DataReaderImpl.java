package com.apptware.interview.stream.impl;

import com.apptware.interview.stream.DataReader;
import com.apptware.interview.stream.PaginationService;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
class DataReaderImpl implements DataReader {
  @Autowired
  private PaginationService paginationService;

  // page size for pagination
  private static final int PAGE_SIZE = 100;

  @Override
  public Stream<String> fetchLimitadData(int limit) {
    // Fetches a limited number of items from the paginated data stream
    return fetchPaginatedDataAsStream().limit(limit);
  }

  @Override
  public Stream<String> fetchFullData() {
    // Fetches all items from the paginated data stream
    return fetchPaginatedDataAsStream();
  }

  private @Nonnull Stream<String> fetchPaginatedDataAsStream() {
    log.info("Fetching paginated data as stream.");

    // Iterator to fetch and track paginated data
    Iterator<String> iterator = new Iterator<String>() {
      private int currentPage = 1;
      private Iterator<String> currentPageIterator = null;

      private boolean loadNextPage() {
        List<String> nextPage = paginationService.getPaginatedData(currentPage, PAGE_SIZE);
        if (nextPage.isEmpty()) {
          return false;
        }
        currentPageIterator = nextPage.iterator();
        return true;
      }

      @Override
      public boolean hasNext() {
        // Checks if there is more data to retrieve
        if (currentPageIterator == null || !currentPageIterator.hasNext()) {
          return loadNextPage() && currentPageIterator.hasNext();
        }
        return true;
      }

      @Override
      public String next() {
        // Fetches the next element, loads a new page if necessary
        if (currentPageIterator == null || !currentPageIterator.hasNext()) {
          if (!loadNextPage()) {
            throw new java.util.NoSuchElementException();
          }
        }
        String next = currentPageIterator.next();
        if (!currentPageIterator.hasNext()) {
          currentPage++;
        }
        return next;
      }
    };

    // Creates a stream that iterates over pages, fetching data from
    // PaginationService
    return Stream.iterate(1, page -> !paginationService.getPaginatedData(page, PAGE_SIZE).isEmpty(), page -> page + 1)
        .flatMap(page -> paginationService.getPaginatedData(page, PAGE_SIZE).stream())
        .peek(item -> log.info("Fetched Item: {}", item));

  }
}
