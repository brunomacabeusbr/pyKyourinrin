<p align="center">
  <a href="">
    <img alt="Logo" src="http://i.imgur.com/JPoMp2D.png" width="500px">
  </a>
</p>

<p align="center">
  An extensible framework for data mining.
</p>

<p align="center">

  <a href="https://github.com/zetaresearch/spyck/blob/master/LICENSE.md"><img alt="License" src="https://img.shields.io/badge/license-MIT-green.svg?style=flat"></a>

</p>

## Table of Contents

- [Purpose](#purpose)
- [Concepts](#concepts)
- [Requirements](#requirements)
- [Other Resources](#other-resources)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [History](#history)
- [License](#license)

## Purpose

*spyck* is a framework which aims to make it easy to develop crawlers and
integrate collected data - independent of its type and origin. It's easily
**expandable** and **adaptable**. It also aims to be easy to use, even for
beginners.

It can be very useful for a wide variety of cases, e.g.:

- Journalist investigations to find corruption cases - like [this one](http://g1.globo.com/jornal-nacional/noticia/2016/01/hospital-do-rj-tem-medico-no-plantao-que-nao-aparece-para-trabalhar.html);
- Researching the population of a particular group;
- Better understanding of a candidate for a job before it hiring
- *etc.*

## Concepts

During the framework development some words got new meanings:

- **Crawler**: The data collector.
- **Harvest**: The execution.
- **Dependencies**: Required previous data.

> Also, each crawler has its *possible-to-achieve* **crop** after the
**harvest**. Each crawler works in one or more different **entities**, where it
contextualizes and store the collected data.

## Requirements

> Everything below can be easily installed via
[setuptools](https://pypi.python.org/pypi/setuptools).

- python 3.x
- requests
- PyPDF2
- selenium
- pyslibtesseract
- aylien-apiclient

The you need to install:

- phantomJS

```sh
sudo apt-get install phantomjs
```

## Other Resources

> Relax, some better docs will come soon.

You can find more info about the framework - and get some feed about its
development through [this blog post](http://macalogs.com.br/spyck-apresentacao-do-framework-de-mineracao-de-dados/).

You can also check the slides from a presentation made at [XI Pylestras](http://pylestras.org/evento/xi-pylestras/)
about the framework [here](http://zetaresearch.github.io/talks/spyck.pdf).

## Roadmap

- [ ] Simplify the code and make it easier to work on the development of the
framework itself.
- [ ] Create a graphical interface (*GUI*) to make it more accessible to
beginners.
- [ ] Implement analysis and inferences about the collected data.

## Contributing

Contributions are very welcome! If you'd like to contribute,
[these guidelines](CONTRIBUTING.md) may help you.

## History

See [Releases](https://github.com/zetaresearch/spyck/releases) for detailed changelog.

## License

[MIT License](LICENSE.md) © ZETA Research.
