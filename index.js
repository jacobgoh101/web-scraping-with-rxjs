const { BehaviorSubject, from, of, timer } = require('rxjs');
const {
  map,
  distinct,
  filter,
  mergeMap,
  retry,
  catchError,
  share,
  retryWhen,
  finalize
} = require('rxjs/operators');
const rp = require('request-promise-native');
const normalizeUrl = require('normalize-url');
const cheerio = require('cheerio');
const { resolve } = require('url');
const fs = require('fs');

const baseUrl = `https://imdb.com`;
const maxConcurrentReq = 10;
const maxRetries = 5;

const genericRetryStrategy = ({
  maxRetryAttempts,
  scalingDuration,
  excludedStatusCodes
}) => attempts => {
  return attempts.pipe(
    mergeMap((error, i) => {
      const retryAttempt = i + 1;
      // if maximum number of retries have been met
      // or response is a status code we don't wish to retry, throw error
      if (
        retryAttempt > maxRetryAttempts ||
        excludedStatusCodes.find(e => e === error.status)
      ) {
        return throwError(error);
      }
      console.log(
        `Attempt ${retryAttempt}: retrying in ${retryAttempt *
          scalingDuration}ms`
      );
      // retry after 1s, 2s, etc...
      return timer(retryAttempt * scalingDuration);
    }),
    finalize(() => console.log('We are done!'))
  );
};

const allUrl$ = new BehaviorSubject(baseUrl);

const uniqueUrl$ = allUrl$.pipe(
  // only crawl IMDB url
  filter(url => url.includes(baseUrl)),
  // normalize url for comparison
  map(url => normalizeUrl(url, { removeQueryParameters: ['ref', 'ref_'] })),
  // distinct is a RxJS operator that filters out duplicated values
  distinct()
);

const urlAndDOM$ = uniqueUrl$.pipe(
  mergeMap(
    url => {
      return from(rp(url)).pipe(
        retryWhen(
          genericRetryStrategy({
            maxRetryAttempts: maxRetries,
            scalingDuration: 3000,
            excludedStatusCodes: []
          })
        ),
        catchError(error => {
          const { uri } = error.options;
          console.log(`Error requesting ${uri} after ${maxRetries} retries.`);
          // return null on error
          return of(null);
        }),
        // filter out errors
        filter(v => v),
        // get the cheerio function $
        map(html => cheerio.load(html)),
        // add URL to the result. It will be used later for crawling
        map($ => ({
          $,
          url
        }))
      );
    },
    null,
    maxConcurrentReq
  ),
  share()
);

// get all the next crawlable URLs
urlAndDOM$.subscribe(({ url, $ }) => {
  $('a').each(function(i, elem) {
    const href = $(this).attr('href');
    if (!href) return;

    // build the absolute url
    const absoluteUrl = resolve(url, href);
    allUrl$.next(absoluteUrl);
  });
});

// scraping for the movies we want
const isMovie = $ =>
  $(`[property='og:type']`).attr('content') === 'video.movie';
const isComedy = $ =>
  $(`.title_wrapper .subtext`)
    .text()
    .includes('Comedy');
const isHighlyRated = $ => +$(`[itemprop="ratingValue"]`).text() > 7;
urlAndDOM$
  .pipe(
    filter(({ $ }) => isMovie($)),
    filter(({ $ }) => isComedy($)),
    filter(({ $ }) => isHighlyRated($))
  )
  .subscribe(({ url, $ }) => {
    // append the data we want to a file named "comedy.txt"
    fs.appendFile('comedy.txt', `${url}, ${$('title').text()}\n`, () => {
      console.log(`appended ${url}`);
    });
  });
