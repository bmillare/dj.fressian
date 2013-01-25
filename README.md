# dj.fressian

A convenience wrapper for fressian, the binary serial/deserializer used by datomic

## Implementation

Currently it is a blatant copy/paste of `org.fressian.api`. This library exists merely for convenience. I also added a handler for vectors.

## Usage

```clojure
(require '[dj.fressian :as df])

(df/poop (clojure.java.io/file "test.bin")
         {:x 3e-9
          'x -1})
(df/eat (clojure.java.io/file "test.bin"))

;;> {:x 3e-9, x -1}
```

## Resources

https://github.com/Datomic/fressian