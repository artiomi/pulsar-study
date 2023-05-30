package my.study.functions;

import java.util.function.Function;

public class GreetingNativeFunction implements Function<String, String> {

  @Override
  public String apply(String input) {
    return String.format("Hello %s!", input);
  }
}
