const { BehaviorSubject, from } = require('rxjs');
const { map, distinct, filter, mergeMap, share } = require('rxjs/operators');
const rp = require('request-promise-native');
const normalizeUrl = require('normalize-url');
const cheerio = require('cheerio');
const { resolve } = require('url');
const fs = require('fs');

const baseUrl = `https://imdb.com`;

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
  mergeMap(url => {
    return from(rp(url)).pipe(
      // get the cheerio function $
      map(html => cheerio.load(html)),
      // add URL to the result. It will be used later for crawling
      map($ => ({
        $,
        url
      }))
    );
  }),
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
