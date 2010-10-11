package AnyEvent::HTTP::Batch;

use Any::Moose;

has conn_limit => (
  is => 'ro',
  isa => 'Int',
  default => 5,
);

has interval_cb => (
  is => 'ro',
  isa => 'CodeRef',
  default => sub {},
);

has complete_cb => (
  is => 'ro',
  isa => 'CodeRef',
  default => sub {},
);

has timeout_cb => (
  is => 'ro',
  isa => 'CodeRef',
  default => sub {warn "batch download timed out\n"},
);

has timeout => (
  is => 'ro',
  isa => 'Int',
  default => 15,
);

sub get {
  my ($self, $urls, %args) = @_;
  $self->request("get", $urls, %args);
}

sub post {
  my ($self, @urls) = @_;
  $self->request("post", $urls, %args);
}

sub head {
  my ($self, @urls) = @_;
  $self->request("head", $urls, %args);
}

sub request {
  my ($self, $method, $urls, %args) = @_;

  my @connections;
  my $open_connections = 0;
  my $count = scalar @urls;
  my ($timer_w, $idle_w);

  my $done = sub {
    @connections = ();
    undef $timer_w;
    undef $idle_w;
  }

  $timer_w = AE::timer 0, $self->timeout, sub {
    $done->();
    $self->timeout_cb();
  };

  $idle_w = AE::idle_w sub {
    return if $open_connections > $self->conn_limit;

    if (!$count) {
      $done->();
      $self->complete_cb();
      return;
    }

    return unless @$urls;
    $open_connections++;

    push @connections, http_request $method, shift @$urls, %args, sub {
      $open_connections--;
      $count--;

      $self->interval_cb(@_);
    };
  };
}

1;
